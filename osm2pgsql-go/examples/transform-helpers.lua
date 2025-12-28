-- Example demonstrating tag transform helpers
-- osm2pgsql-go provides built-in functions for common tag transformations

-- Access transforms via osm2pgsql.transforms or convenient global shortcuts
local transforms = osm2pgsql.transforms

-- Define a roads table with computed columns
local roads = osm2pgsql.define_table({
    name = 'roads',
    ids = { type = 'way' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'name_en', type = 'text' },
        { column = 'highway', type = 'text' },
        { column = 'ref', type = 'text' },
        { column = 'oneway', type = 'int' },
        { column = 'layer', type = 'int' },
        { column = 'z_order', type = 'int' },
        { column = 'tags', type = 'jsonb' },
        { column = 'geom', type = 'linestring' },
    }
})

-- Define a POI table
local pois = osm2pgsql.define_table({
    name = 'pois',
    ids = { type = 'node' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'name_local', type = 'text' },
        { column = 'amenity', type = 'text' },
        { column = 'shop', type = 'text' },
        { column = 'is_wheelchair', type = 'boolean' },
        { column = 'opening_hours', type = 'text' },
        { column = 'tags', type = 'jsonb' },
        { column = 'geom', type = 'point' },
    }
})

-- Define buildings table
local buildings = osm2pgsql.define_table({
    name = 'buildings',
    ids = { type = 'area' },
    columns = {
        { column = 'name', type = 'text' },
        { column = 'building_type', type = 'text' },
        { column = 'levels', type = 'int' },
        { column = 'height', type = 'real' },
        { column = 'filtered_tags', type = 'jsonb' },
        { column = 'geom', type = 'polygon' },
    }
})

-- Tags to keep for buildings
local building_keep_tags = {"name", "addr:street", "addr:housenumber", "addr:city"}

-- Process nodes (POIs)
function osm2pgsql.process_node(object)
    local tags = object.tags

    -- Skip if no useful tags
    if not tags.amenity and not tags.shop and not tags.tourism then
        return
    end

    pois:insert({
        -- get_name tries: name, int_name, name:en
        name = get_name(tags),

        -- Get localized name with fallback
        name_local = transforms.get_name_localized(tags, "de"),

        amenity = tags.amenity,
        shop = tags.shop,

        -- Parse boolean value (yes/no/true/false/1/0)
        is_wheelchair = parse_bool(tags.wheelchair or "no"),

        -- Clean and truncate opening hours
        opening_hours = transforms.truncate(
            transforms.clean_spaces(tags.opening_hours or ""),
            255
        ),

        -- Convert remaining tags to JSON
        tags = transforms.tags_to_json(tags),

        geom = object:as_point()
    })
end

-- Process ways (roads and buildings)
function osm2pgsql.process_way(object)
    local tags = object.tags

    -- Process roads
    if tags.highway then
        roads:insert({
            -- Clean name whitespace
            name = transforms.clean_spaces(tags.name or ""),

            -- Get English name with fallback
            name_en = transforms.get_name_localized(tags, "en"),

            highway = transforms.lower(tags.highway),

            -- Trim and truncate ref
            ref = transforms.truncate(trim(tags.ref or ""), 50),

            -- Parse oneway direction: 1, -1, or 0
            oneway = transforms.parse_direction(tags.oneway or ""),

            -- Parse layer with clamping (-10 to 10)
            layer = transforms.parse_layer(tags.layer or "0"),

            -- Calculate z_order for rendering (handles bridge/tunnel/layer)
            z_order = transforms.calc_z_order(tags),

            -- Store all tags as JSON
            tags = transforms.tags_to_json(tags),

            geom = object:as_linestring()
        })
    end

    -- Process buildings (closed ways)
    if tags.building and object.is_closed then
        -- Check if it's really an area
        if transforms.is_area(tags, object.is_closed) then
            buildings:insert({
                name = get_name(tags),

                -- Normalize building type to lowercase
                building_type = transforms.lower(tags.building),

                -- Parse number of levels (defaults to 0)
                levels = parse_int(tags["building:levels"] or "0"),

                -- Parse height as float
                height = transforms.parse_real(tags.height or "0"),

                -- Filter tags to only keep relevant ones
                filtered_tags = transforms.tags_to_json(
                    transforms.filter_tags(tags, building_keep_tags)
                ),

                geom = object:as_polygon()
            })
        end
    end
end

-- Process relations (multipolygon buildings)
function osm2pgsql.process_relation(object)
    local tags = object.tags

    if tags.type ~= 'multipolygon' then
        return
    end

    if tags.building then
        buildings:insert({
            name = get_name(tags),
            building_type = transforms.lower(tags.building),
            levels = parse_int(tags["building:levels"] or "0"),
            height = transforms.parse_real(tags.height or "0"),
            filtered_tags = transforms.tags_to_json(
                transforms.filter_tags(tags, building_keep_tags)
            ),
            geom = object:as_multipolygon()
        })
    end
end
