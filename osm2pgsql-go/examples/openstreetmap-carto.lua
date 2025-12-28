-- OpenStreetMap Carto-like Lua Flex style for osm2pgsql-go
-- This creates tables suitable for rendering with standard OSM styles

-- Helper function to check if a value is in a list
local function contains(list, value)
    for _, v in ipairs(list) do
        if v == value then
            return true
        end
    end
    return false
end

-- Define roads table
local planet_osm_roads = osm2pgsql.define_table({
    name = 'planet_osm_roads',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'railway', type = 'text' },
        { column = 'ref', type = 'text' },
        { column = 'layer', type = 'int' },
        { column = 'z_order', type = 'int' },
        { column = 'geom', type = 'linestring' },
    },
    indexes = {
        { column = 'highway' },
    }
})

-- Define line features table
local planet_osm_line = osm2pgsql.define_table({
    name = 'planet_osm_line',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'railway', type = 'text' },
        { column = 'waterway', type = 'text' },
        { column = 'barrier', type = 'text' },
        { column = 'natural', type = 'text' },
        { column = 'power', type = 'text' },
        { column = 'tags', type = 'jsonb' },
        { column = 'layer', type = 'int' },
        { column = 'z_order', type = 'int' },
        { column = 'geom', type = 'linestring' },
    }
})

-- Define polygon features table
local planet_osm_polygon = osm2pgsql.define_table({
    name = 'planet_osm_polygon',
    ids = { type = 'area' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'building', type = 'text' },
        { column = 'landuse', type = 'text' },
        { column = 'natural', type = 'text' },
        { column = 'water', type = 'text' },
        { column = 'leisure', type = 'text' },
        { column = 'amenity', type = 'text' },
        { column = 'tags', type = 'jsonb' },
        { column = 'layer', type = 'int' },
        { column = 'z_order', type = 'int' },
        { column = 'geom', type = 'polygon' },
    }
})

-- Define point features table
local planet_osm_point = osm2pgsql.define_table({
    name = 'planet_osm_point',
    ids = { type = 'node' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'amenity', type = 'text' },
        { column = 'shop', type = 'text' },
        { column = 'tourism', type = 'text' },
        { column = 'natural', type = 'text' },
        { column = 'place', type = 'text' },
        { column = 'tags', type = 'jsonb' },
        { column = 'geom', type = 'point' },
    }
})

-- Road z_order values (higher = rendered on top)
local highway_z_order = {
    motorway = 380,
    trunk = 370,
    primary = 360,
    secondary = 350,
    tertiary = 340,
    residential = 330,
    unclassified = 330,
    road = 330,
    living_street = 320,
    pedestrian = 310,
    service = 300,
    track = 290,
    path = 280,
    footway = 280,
    cycleway = 280,
    bridleway = 280,
    steps = 270,
}

local railway_z_order = {
    rail = 440,
    subway = 420,
    tram = 410,
    light_rail = 430,
}

-- Calculate z_order for a way
local function calc_z_order(tags)
    local z = 0

    if tags.highway then
        z = highway_z_order[tags.highway] or 300
    elseif tags.railway then
        z = railway_z_order[tags.railway] or 400
    end

    -- Layer adjustment
    local layer = tonumber(tags.layer) or 0
    z = z + layer * 10

    -- Bridge/tunnel adjustment
    if tags.bridge and tags.bridge ~= 'no' then
        z = z + 100
    elseif tags.tunnel and tags.tunnel ~= 'no' then
        z = z - 100
    end

    return z
end

-- Roads that go into the roads table (for low-zoom rendering)
local major_roads = {
    'motorway', 'motorway_link',
    'trunk', 'trunk_link',
    'primary', 'primary_link',
    'secondary', 'secondary_link',
    'tertiary', 'tertiary_link',
}

-- Check if tags contain any meaningful values
local function has_tags(tags)
    for k, v in pairs(tags) do
        if k ~= 'source' and k ~= 'created_by' and k ~= 'note' then
            return true
        end
    end
    return false
end

-- Process nodes
function osm2pgsql.process_node(object)
    if not has_tags(object.tags) then
        return
    end

    planet_osm_point:insert({
        name = object.tags.name,
        amenity = object.tags.amenity,
        shop = object.tags.shop,
        tourism = object.tags.tourism,
        natural = object.tags.natural,
        place = object.tags.place,
        tags = object.tags,
        geom = object:as_point()
    })
end

-- Process ways
function osm2pgsql.process_way(object)
    local tags = object.tags
    local z_order = calc_z_order(tags)
    local layer = tonumber(tags.layer) or 0

    -- Check if this is a linear feature
    local is_line = tags.highway or tags.railway or tags.waterway or
                    tags.barrier or tags.power

    -- Check if this is an area feature
    local is_area = object.is_closed and (
        tags.building or tags.landuse or tags.natural or
        tags.water or tags.leisure or tags.amenity or
        tags.area == 'yes'
    )

    if is_line and not is_area then
        -- Insert into line table
        planet_osm_line:insert({
            name = tags.name,
            highway = tags.highway,
            railway = tags.railway,
            waterway = tags.waterway,
            barrier = tags.barrier,
            natural = tags.natural,
            power = tags.power,
            tags = tags,
            layer = layer,
            z_order = z_order,
            geom = object:as_linestring()
        })

        -- Also insert major roads into roads table
        if tags.highway and contains(major_roads, tags.highway) then
            planet_osm_roads:insert({
                name = tags.name,
                highway = tags.highway,
                railway = tags.railway,
                ref = tags.ref,
                layer = layer,
                z_order = z_order,
                geom = object:as_linestring()
            })
        end
        if tags.railway then
            planet_osm_roads:insert({
                name = tags.name,
                highway = tags.highway,
                railway = tags.railway,
                ref = tags.ref,
                layer = layer,
                z_order = z_order,
                geom = object:as_linestring()
            })
        end
    end

    if is_area then
        planet_osm_polygon:insert({
            name = tags.name,
            building = tags.building,
            landuse = tags.landuse,
            natural = tags.natural,
            water = tags.water,
            leisure = tags.leisure,
            amenity = tags.amenity,
            tags = tags,
            layer = layer,
            z_order = z_order,
            geom = object:as_polygon()
        })
    end
end

-- Process relations
function osm2pgsql.process_relation(object)
    local tags = object.tags

    if tags.type ~= 'multipolygon' and tags.type ~= 'boundary' then
        return
    end

    local z_order = calc_z_order(tags)
    local layer = tonumber(tags.layer) or 0

    planet_osm_polygon:insert({
        name = tags.name,
        building = tags.building,
        landuse = tags.landuse,
        natural = tags.natural,
        water = tags.water,
        leisure = tags.leisure,
        amenity = tags.amenity,
        tags = tags,
        layer = layer,
        z_order = z_order,
        geom = object:as_multipolygon()
    })
end
