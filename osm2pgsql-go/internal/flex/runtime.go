package flex

import (
	"fmt"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
)

// Runtime manages the Lua interpreter and osm2pgsql API
type Runtime struct {
	L              *lua.LState
	tables         *TableRegistry
	currentObject  *OSMObject
	pendingRows    []Row
	mu             sync.Mutex
	srid           int
	schema         string
	processNode    lua.LValue
	processWay     lua.LValue
	processRelation lua.LValue
}

// NewRuntime creates a new Lua runtime with osm2pgsql API
func NewRuntime(srid int, schema string) *Runtime {
	L := lua.NewState(lua.Options{
		SkipOpenLibs: false,
	})

	r := &Runtime{
		L:      L,
		tables: NewTableRegistry(),
		srid:   srid,
		schema: schema,
	}

	r.registerAPI()
	return r
}

// Close releases Lua resources
func (r *Runtime) Close() {
	r.L.Close()
}

// registerAPI registers the osm2pgsql Lua API
func (r *Runtime) registerAPI() {
	// Create osm2pgsql module table
	osm2pgsql := r.L.NewTable()

	// Add version info
	osm2pgsql.RawSetString("version", lua.LString("1.0.0"))
	osm2pgsql.RawSetString("mode", lua.LString("flex"))

	// Add SRID
	osm2pgsql.RawSetString("srid", lua.LNumber(r.srid))

	// Add table definition function
	r.L.SetField(osm2pgsql, "define_table", r.L.NewFunction(r.defineTable))

	// Add stage constants
	osm2pgsql.RawSetString("stage", lua.LNumber(1)) // 1 = processing stage

	// Set osm2pgsql as global
	r.L.SetGlobal("osm2pgsql", osm2pgsql)

	// Register tag transform helpers (adds osm2pgsql.transforms and convenience globals)
	RegisterTransforms(r.L)

	// Register helper functions
	r.L.SetGlobal("print", r.L.NewFunction(r.luaPrint))
}

// LoadFile loads and executes a Lua style file
func (r *Runtime) LoadFile(path string) error {
	if err := r.L.DoFile(path); err != nil {
		return fmt.Errorf("failed to load Lua file: %w", err)
	}

	r.extractCallbacks()
	return nil
}

// LoadString loads and executes Lua code from a string (for testing)
func (r *Runtime) LoadString(code string) error {
	if err := r.L.DoString(code); err != nil {
		return fmt.Errorf("failed to load Lua code: %w", err)
	}

	r.extractCallbacks()
	return nil
}

// extractCallbacks extracts process callbacks from osm2pgsql table
func (r *Runtime) extractCallbacks() {
	osm2pgsql := r.L.GetGlobal("osm2pgsql")
	if osm2pgsql.Type() == lua.LTTable {
		tbl := osm2pgsql.(*lua.LTable)
		r.processNode = tbl.RawGetString("process_node")
		r.processWay = tbl.RawGetString("process_way")
		r.processRelation = tbl.RawGetString("process_relation")
	}
}

// defineTable implements osm2pgsql.define_table()
func (r *Runtime) defineTable(L *lua.LState) int {
	tbl := L.CheckTable(1)

	table := &Table{
		Schema: r.schema,
	}

	// Parse table name
	if name := tbl.RawGetString("name"); name.Type() == lua.LTString {
		table.Name = string(name.(lua.LString))
	} else {
		L.RaiseError("table name is required")
		return 0
	}

	// Parse schema override
	if schema := tbl.RawGetString("schema"); schema.Type() == lua.LTString {
		table.Schema = string(schema.(lua.LString))
	}

	// Parse IDs column (special handling)
	if ids := tbl.RawGetString("ids"); ids.Type() == lua.LTTable {
		idsTbl := ids.(*lua.LTable)
		typeVal := idsTbl.RawGetString("type")
		if typeVal.Type() == lua.LTString {
			idType := string(typeVal.(lua.LString))
			switch idType {
			case "node", "way", "relation", "area", "any":
				// Add osm_id column
				table.Columns = append(table.Columns, Column{
					Name:    "osm_id",
					Type:    ColumnTypeBigInt,
					NotNull: true,
				})
				// Add osm_type for "any" and "area"
				if idType == "any" || idType == "area" {
					table.Columns = append(table.Columns, Column{
						Name:    "osm_type",
						Type:    ColumnTypeText,
						NotNull: false,
					})
				}
			}
		}
	}

	// Parse columns
	if columns := tbl.RawGetString("columns"); columns.Type() == lua.LTTable {
		colsTbl := columns.(*lua.LTable)
		colsTbl.ForEach(func(_, colDef lua.LValue) {
			if colDef.Type() != lua.LTTable {
				return
			}
			col := r.parseColumn(colDef.(*lua.LTable))
			table.Columns = append(table.Columns, col)
			if col.Type >= ColumnTypeGeometry && col.Type <= ColumnTypeGeometryCollection {
				table.GeomColumn = col.Name
				table.SRID = r.srid
				if col.SRID != 0 {
					table.SRID = col.SRID
				}
			} else {
				table.DataColumns = append(table.DataColumns, col.Name)
			}
		})
	}

	// Parse indexes
	if indexes := tbl.RawGetString("indexes"); indexes.Type() == lua.LTTable {
		idxTbl := indexes.(*lua.LTable)
		idxTbl.ForEach(func(_, idx lua.LValue) {
			if idx.Type() == lua.LTTable {
				idxDef := idx.(*lua.LTable)
				if col := idxDef.RawGetString("column"); col.Type() == lua.LTString {
					table.Indexes = append(table.Indexes, string(col.(lua.LString)))
				}
			}
		})
	}

	// Parse cluster
	if cluster := tbl.RawGetString("cluster"); cluster.Type() == lua.LTString {
		table.ClusterOn = string(cluster.(lua.LString))
	}

	r.tables.Register(table)

	// Return a table object that can receive inserts
	tableLua := L.NewTable()
	L.SetField(tableLua, "name", lua.LString(table.Name))
	L.SetField(tableLua, "insert", L.NewFunction(r.tableInsert(table)))

	// Push the table onto the stack to return it
	L.Push(tableLua)
	return 1
}

// parseColumn parses a column definition table
func (r *Runtime) parseColumn(tbl *lua.LTable) Column {
	col := Column{
		SRID: r.srid,
	}

	if name := tbl.RawGetString("column"); name.Type() == lua.LTString {
		col.Name = string(name.(lua.LString))
	}

	if typ := tbl.RawGetString("type"); typ.Type() == lua.LTString {
		typStr := strings.ToLower(string(typ.(lua.LString)))
		colType, _ := ParseColumnType(typStr)
		col.Type = colType
	}

	if notNull := tbl.RawGetString("not_null"); notNull.Type() == lua.LTBool {
		col.NotNull = bool(notNull.(lua.LBool))
	}

	if createOnly := tbl.RawGetString("create_only"); createOnly.Type() == lua.LTBool {
		col.CreateOnly = bool(createOnly.(lua.LBool))
	}

	if srid := tbl.RawGetString("srid"); srid.Type() == lua.LTNumber {
		col.SRID = int(srid.(lua.LNumber))
	}

	return col
}

// tableInsert creates the insert method for a table
func (r *Runtime) tableInsert(table *Table) lua.LGFunction {
	return func(L *lua.LState) int {
		// When called as table:insert(data), the first arg is self (the table)
		// and the second arg is the row data
		var rowData *lua.LTable
		if L.GetTop() >= 2 {
			rowData = L.CheckTable(2)
		} else {
			rowData = L.CheckTable(1)
		}

		row := Row{
			TableName: table.Name,
			Values:    make(map[string]interface{}),
		}

		// Extract values from the Lua table
		rowData.ForEach(func(key, value lua.LValue) {
			if key.Type() != lua.LTString {
				return
			}
			keyStr := string(key.(lua.LString))

			switch v := value.(type) {
			case lua.LString:
				row.Values[keyStr] = string(v)
			case lua.LNumber:
				row.Values[keyStr] = float64(v)
			case lua.LBool:
				row.Values[keyStr] = bool(v)
			case *lua.LTable:
				// Check if it's a geometry userdata
				if geomData := v.RawGetString("_wkb"); geomData.Type() == lua.LTString {
					row.GeomWKB = []byte(string(geomData.(lua.LString)))
				} else {
					// Regular table - convert to JSON-like structure
					row.Values[keyStr] = r.tableToMap(v)
				}
			case *lua.LUserData:
				// Could be geometry or other userdata
				if wkb, ok := v.Value.([]byte); ok {
					row.GeomWKB = wkb
				}
			case *lua.LNilType:
				row.Values[keyStr] = nil
			}
		})

		// If the table has osm_id column and we have a current object, add it
		if r.currentObject != nil {
			for _, col := range table.Columns {
				if col.Name == "osm_id" {
					row.Values["osm_id"] = r.currentObject.ID
				}
				if col.Name == "osm_type" {
					row.Values["osm_type"] = r.currentObject.Type
				}
			}
		}

		// Check for geom column and use current object's geometry if not set
		if row.GeomWKB == nil && table.GeomColumn != "" && r.currentObject != nil {
			if wkb, _ := r.currentObject.Geometry(); wkb != nil {
				row.GeomWKB = wkb
			}
		}

		r.mu.Lock()
		r.pendingRows = append(r.pendingRows, row)
		r.mu.Unlock()

		return 0
	}
}

// tableToMap converts a Lua table to a Go map
func (r *Runtime) tableToMap(tbl *lua.LTable) map[string]interface{} {
	result := make(map[string]interface{})
	tbl.ForEach(func(key, value lua.LValue) {
		if key.Type() != lua.LTString {
			return
		}
		keyStr := string(key.(lua.LString))
		switch v := value.(type) {
		case lua.LString:
			result[keyStr] = string(v)
		case lua.LNumber:
			result[keyStr] = float64(v)
		case lua.LBool:
			result[keyStr] = bool(v)
		case *lua.LTable:
			result[keyStr] = r.tableToMap(v)
		}
	})
	return result
}

// luaPrint implements the print function for Lua
func (r *Runtime) luaPrint(L *lua.LState) int {
	n := L.GetTop()
	var parts []string
	for i := 1; i <= n; i++ {
		parts = append(parts, L.ToStringMeta(L.Get(i)).String())
	}
	fmt.Println(strings.Join(parts, "\t"))
	return 0
}

// Tables returns the table registry
func (r *Runtime) Tables() *TableRegistry {
	return r.tables
}

// SetCurrentObject sets the current OSM object being processed
func (r *Runtime) SetCurrentObject(obj *OSMObject) {
	r.currentObject = obj
}

// CollectRows returns and clears pending rows
func (r *Runtime) CollectRows() []Row {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := r.pendingRows
	r.pendingRows = nil
	return rows
}

// HasProcessNode returns true if process_node callback is defined
func (r *Runtime) HasProcessNode() bool {
	return r.processNode != nil && r.processNode.Type() == lua.LTFunction
}

// HasProcessWay returns true if process_way callback is defined
func (r *Runtime) HasProcessWay() bool {
	return r.processWay != nil && r.processWay.Type() == lua.LTFunction
}

// HasProcessRelation returns true if process_relation callback is defined
func (r *Runtime) HasProcessRelation() bool {
	return r.processRelation != nil && r.processRelation.Type() == lua.LTFunction
}

// ProcessNode calls the process_node callback
func (r *Runtime) ProcessNode(obj *OSMObject) error {
	if !r.HasProcessNode() {
		return nil
	}
	return r.callProcessCallback(r.processNode, obj)
}

// ProcessWay calls the process_way callback
func (r *Runtime) ProcessWay(obj *OSMObject) error {
	if !r.HasProcessWay() {
		return nil
	}
	return r.callProcessCallback(r.processWay, obj)
}

// ProcessRelation calls the process_relation callback
func (r *Runtime) ProcessRelation(obj *OSMObject) error {
	if !r.HasProcessRelation() {
		return nil
	}
	return r.callProcessCallback(r.processRelation, obj)
}

// callProcessCallback invokes a Lua process callback with an object
func (r *Runtime) callProcessCallback(fn lua.LValue, obj *OSMObject) error {
	r.SetCurrentObject(obj)

	// Create object table for Lua
	objTable := r.objectToLua(obj)

	if err := r.L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    0,
		Protect: true,
	}, objTable); err != nil {
		return fmt.Errorf("lua callback error: %w", err)
	}

	return nil
}

// objectToLua converts an OSMObject to a Lua table
func (r *Runtime) objectToLua(obj *OSMObject) *lua.LTable {
	L := r.L
	tbl := L.NewTable()

	// Basic properties
	tbl.RawSetString("id", lua.LNumber(obj.ID))
	tbl.RawSetString("type", lua.LString(obj.Type))
	tbl.RawSetString("version", lua.LNumber(obj.Version))
	tbl.RawSetString("changeset", lua.LNumber(obj.Changeset))
	tbl.RawSetString("uid", lua.LNumber(obj.UID))
	tbl.RawSetString("user", lua.LString(obj.User))

	// Tags as a table
	tags := L.NewTable()
	for k, v := range obj.Tags {
		tags.RawSetString(k, lua.LString(v))
	}
	tbl.RawSetString("tags", tags)

	// Type-specific properties
	switch obj.Type {
	case "node":
		tbl.RawSetString("lat", lua.LNumber(obj.Lat))
		tbl.RawSetString("lon", lua.LNumber(obj.Lon))
	case "way":
		tbl.RawSetString("is_closed", lua.LBool(obj.IsClosed))
		nodes := L.NewTable()
		for i, nodeRef := range obj.NodeRefs {
			nodes.RawSetInt(i+1, lua.LNumber(nodeRef))
		}
		tbl.RawSetString("nodes", nodes)
	case "relation":
		members := L.NewTable()
		for i, m := range obj.Members {
			memberTbl := L.NewTable()
			memberTbl.RawSetString("type", lua.LString(m.Type))
			memberTbl.RawSetString("ref", lua.LNumber(m.Ref))
			memberTbl.RawSetString("role", lua.LString(m.Role))
			members.RawSetInt(i+1, memberTbl)
		}
		tbl.RawSetString("members", members)
	}

	// Add helper methods
	L.SetField(tbl, "grab_tag", L.NewFunction(r.grabTag(obj)))
	L.SetField(tbl, "as_point", L.NewFunction(r.asPoint(obj)))
	L.SetField(tbl, "as_linestring", L.NewFunction(r.asLineString(obj)))
	L.SetField(tbl, "as_polygon", L.NewFunction(r.asPolygon(obj)))
	L.SetField(tbl, "as_multilinestring", L.NewFunction(r.asMultiLineString(obj)))
	L.SetField(tbl, "as_multipolygon", L.NewFunction(r.asMultiPolygon(obj)))
	L.SetField(tbl, "as_area", L.NewFunction(r.asArea(obj)))

	return tbl
}

// grabTag implements object:grab_tag()
func (r *Runtime) grabTag(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		key := L.CheckString(1)
		if val, ok := obj.Tags[key]; ok {
			delete(obj.Tags, key) // "grab" removes the tag
			L.Push(lua.LString(val))
		} else {
			L.Push(lua.LNil)
		}
		return 1
	}
}

// asPoint implements object:as_point()
func (r *Runtime) asPoint(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		if obj.Type != "node" {
			L.Push(lua.LNil)
			return 1
		}
		// Return geometry placeholder - actual encoding done by processor
		geom := L.NewTable()
		geom.RawSetString("_type", lua.LString("point"))
		geom.RawSetString("_lat", lua.LNumber(obj.Lat))
		geom.RawSetString("_lon", lua.LNumber(obj.Lon))
		L.Push(geom)
		return 1
	}
}

// asLineString implements object:as_linestring()
func (r *Runtime) asLineString(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		if obj.Type != "way" || len(obj.Coords) < 4 {
			L.Push(lua.LNil)
			return 1
		}
		geom := L.NewTable()
		geom.RawSetString("_type", lua.LString("linestring"))
		L.Push(geom)
		return 1
	}
}

// asPolygon implements object:as_polygon()
func (r *Runtime) asPolygon(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		if obj.Type != "way" || !obj.IsClosed || len(obj.Coords) < 8 {
			L.Push(lua.LNil)
			return 1
		}
		geom := L.NewTable()
		geom.RawSetString("_type", lua.LString("polygon"))
		L.Push(geom)
		return 1
	}
}

// asMultiLineString implements object:as_multilinestring()
func (r *Runtime) asMultiLineString(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		// For relations with way members
		if obj.Type != "relation" {
			L.Push(lua.LNil)
			return 1
		}
		geom := L.NewTable()
		geom.RawSetString("_type", lua.LString("multilinestring"))
		L.Push(geom)
		return 1
	}
}

// asMultiPolygon implements object:as_multipolygon()
func (r *Runtime) asMultiPolygon(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		if obj.Type != "relation" {
			L.Push(lua.LNil)
			return 1
		}
		geom := L.NewTable()
		geom.RawSetString("_type", lua.LString("multipolygon"))
		L.Push(geom)
		return 1
	}
}

// asArea implements object:as_area() - for ways/relations that should be polygons
func (r *Runtime) asArea(obj *OSMObject) lua.LGFunction {
	return func(L *lua.LState) int {
		switch obj.Type {
		case "way":
			if !obj.IsClosed || len(obj.Coords) < 8 {
				L.Push(lua.LNil)
				return 1
			}
			geom := L.NewTable()
			geom.RawSetString("_type", lua.LString("polygon"))
			L.Push(geom)
		case "relation":
			geom := L.NewTable()
			geom.RawSetString("_type", lua.LString("multipolygon"))
			L.Push(geom)
		default:
			L.Push(lua.LNil)
		}
		return 1
	}
}
