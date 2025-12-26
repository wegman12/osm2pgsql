package parquet

import (
	"encoding/json"
	"os"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/paulmach/osm"
)

// TagsToJSON converts OSM tags to a JSON string (exported for use by pbf package)
func TagsToJSON(tags osm.Tags) string {
	if len(tags) == 0 {
		return "{}"
	}
	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[tag.Key] = tag.Value
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// tagsToJSON is an alias for internal use
func tagsToJSON(tags osm.Tags) string {
	return TagsToJSON(tags)
}

// GeometryWriter writes geometries with WKT to Parquet (legacy)
type GeometryWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewGeometryWriter creates a new geometry Parquet writer
func NewGeometryWriter(path string, batchSize int) (*GeometryWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "osm_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "osm_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "geom_wkt", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &GeometryWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a geometry record
func (w *GeometryWriter) Write(osmID int64, osmType string, tags string, geomWKT string) error {
	w.builder.Field(0).(*array.Int64Builder).Append(osmID)
	w.builder.Field(1).(*array.StringBuilder).Append(osmType)
	w.builder.Field(2).(*array.StringBuilder).Append(tags)
	w.builder.Field(3).(*array.StringBuilder).Append(geomWKT)

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *GeometryWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *GeometryWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// WKBGeometryWriter writes geometries with WKB (binary) to Parquet
// This is more compact and faster to parse than WKT
type WKBGeometryWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewWKBGeometryWriter creates a new WKB geometry Parquet writer
func NewWKBGeometryWriter(path string, batchSize int) (*WKBGeometryWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "osm_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "osm_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "geom_wkb", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &WKBGeometryWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a geometry record with WKB binary geometry
func (w *WKBGeometryWriter) Write(osmID int64, osmType string, tags string, geomWKB []byte) error {
	w.builder.Field(0).(*array.Int64Builder).Append(osmID)
	w.builder.Field(1).(*array.StringBuilder).Append(osmType)
	w.builder.Field(2).(*array.StringBuilder).Append(tags)
	w.builder.Field(3).(*array.BinaryBuilder).Append(geomWKB)

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *WKBGeometryWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *WKBGeometryWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// NodeWriter writes nodes to Parquet
type NodeWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewNodeWriter creates a new node Parquet writer
func NewNodeWriter(path string, batchSize int) (*NodeWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "lat", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "lon", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &NodeWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a node
func (w *NodeWriter) Write(n *osm.Node) error {
	w.builder.Field(0).(*array.Int64Builder).Append(int64(n.ID))
	w.builder.Field(1).(*array.Float64Builder).Append(n.Lat)
	w.builder.Field(2).(*array.Float64Builder).Append(n.Lon)
	w.builder.Field(3).(*array.StringBuilder).Append(tagsToJSON(n.Tags))

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *NodeWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *NodeWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// WayWriter writes ways to Parquet
type WayWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewWayWriter creates a new way Parquet writer
func NewWayWriter(path string, batchSize int) (*WayWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &WayWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a way
func (w *WayWriter) Write(way *osm.Way) error {
	w.builder.Field(0).(*array.Int64Builder).Append(int64(way.ID))
	w.builder.Field(1).(*array.StringBuilder).Append(tagsToJSON(way.Tags))

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *WayWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *WayWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// WayNodeWriter writes way-node relationships to Parquet
type WayNodeWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewWayNodeWriter creates a new way-node Parquet writer
func NewWayNodeWriter(path string, batchSize int) (*WayNodeWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "way_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "seq", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "node_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &WayNodeWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a way-node relationship
func (w *WayNodeWriter) Write(wayID int64, seq int32, nodeID int64) error {
	w.builder.Field(0).(*array.Int64Builder).Append(wayID)
	w.builder.Field(1).(*array.Int32Builder).Append(seq)
	w.builder.Field(2).(*array.Int64Builder).Append(nodeID)

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *WayNodeWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *WayNodeWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// RelationWriter writes relations to Parquet
type RelationWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewRelationWriter creates a new relation Parquet writer
func NewRelationWriter(path string, batchSize int) (*RelationWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &RelationWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a relation
func (w *RelationWriter) Write(rel *osm.Relation) error {
	w.builder.Field(0).(*array.Int64Builder).Append(int64(rel.ID))
	w.builder.Field(1).(*array.StringBuilder).Append(tagsToJSON(rel.Tags))

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *RelationWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *RelationWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}

// RelationMemberWriter writes relation members to Parquet
type RelationMemberWriter struct {
	file      *os.File
	writer    *pqarrow.FileWriter
	builder   *array.RecordBuilder
	batchSize int
	count     int
}

// NewRelationMemberWriter creates a new relation member Parquet writer
func NewRelationMemberWriter(path string, batchSize int) (*RelationMemberWriter, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "relation_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "seq", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ref", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "role", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
	)

	writer, err := pqarrow.NewFileWriter(schema, f, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	return &RelationMemberWriter{
		file:      f,
		writer:    writer,
		builder:   builder,
		batchSize: batchSize,
	}, nil
}

// Write writes a relation member
func (w *RelationMemberWriter) Write(relationID int64, seq int32, memberType string, ref int64, role string) error {
	w.builder.Field(0).(*array.Int64Builder).Append(relationID)
	w.builder.Field(1).(*array.Int32Builder).Append(seq)
	w.builder.Field(2).(*array.StringBuilder).Append(memberType)
	w.builder.Field(3).(*array.Int64Builder).Append(ref)
	w.builder.Field(4).(*array.StringBuilder).Append(role)

	w.count++
	if w.count >= w.batchSize {
		return w.flush()
	}
	return nil
}

func (w *RelationMemberWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	rec := w.builder.NewRecord()
	defer rec.Release()
	err := w.writer.Write(rec)
	w.count = 0
	return err
}

// Close closes the writer
func (w *RelationMemberWriter) Close() error {
	if err := w.flush(); err != nil {
		return err
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.file.Close()
}
