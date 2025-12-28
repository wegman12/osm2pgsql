-- Simple Lua Flex style for osm2pgsql-go
-- This creates a roads table from highway features

-- Define the roads table
local roads = osm2pgsql.define_table({
    name = 'roads',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'surface', type = 'text' },
        { column = 'lanes', type = 'int' },
        { column = 'maxspeed', type = 'text' },
        { column = 'oneway', type = 'boolean' },
        { column = 'geom', type = 'linestring' },
    }
})

-- Define POIs table
local pois = osm2pgsql.define_table({
    name = 'pois',
    ids = { type = 'node' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'amenity', type = 'text' },
        { column = 'shop', type = 'text' },
        { column = 'tourism', type = 'text' },
        { column = 'geom', type = 'point' },
    }
})

-- Define buildings table
local buildings = osm2pgsql.define_table({
    name = 'buildings',
    ids = { type = 'area' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'building', type = 'text' },
        { column = 'height', type = 'text' },
        { column = 'levels', type = 'int' },
        { column = 'geom', type = 'polygon' },
    }
})

-- Process nodes - extract POIs
function osm2pgsql.process_node(object)
    -- Skip nodes without interesting tags
    if not object.tags.amenity and not object.tags.shop and not object.tags.tourism then
        return
    end

    pois:insert({
        name = object.tags.name,
        amenity = object.tags.amenity,
        shop = object.tags.shop,
        tourism = object.tags.tourism,
        geom = object:as_point()
    })
end

-- Process ways - extract roads and buildings
function osm2pgsql.process_way(object)
    -- Extract highways as roads
    if object.tags.highway then
        local oneway = object.tags.oneway == 'yes'
        local lanes = nil
        if object.tags.lanes then
            lanes = tonumber(object.tags.lanes)
        end

        roads:insert({
            name = object.tags.name,
            highway = object.tags.highway,
            surface = object.tags.surface,
            lanes = lanes,
            maxspeed = object.tags.maxspeed,
            oneway = oneway,
            geom = object:as_linestring()
        })
    end

    -- Extract buildings
    if object.tags.building and object.is_closed then
        local levels = nil
        if object.tags['building:levels'] then
            levels = tonumber(object.tags['building:levels'])
        end

        buildings:insert({
            name = object.tags.name,
            building = object.tags.building,
            height = object.tags.height,
            levels = levels,
            geom = object:as_polygon()
        })
    end
end

-- Process relations - extract multipolygon buildings
function osm2pgsql.process_relation(object)
    -- Check if it's a building multipolygon
    if object.tags.type == 'multipolygon' and object.tags.building then
        local levels = nil
        if object.tags['building:levels'] then
            levels = tonumber(object.tags['building:levels'])
        end

        buildings:insert({
            name = object.tags.name,
            building = object.tags.building,
            height = object.tags.height,
            levels = levels,
            geom = object:as_multipolygon()
        })
    end
end
