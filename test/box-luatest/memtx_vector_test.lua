local server = require('luatest.server')
local t = require('luatest')

local function before_all(cg)
    cg.server = server:new({alias = 'master'})
    cg.server:start()
end

local function after_all(cg)
    cg.server:stop()
    t.assert(true, "test suite finished")
end

local function before_each(cg)
    cg.server:exec(function()
        if not box.space.vectors then
            local space = box.schema.space.create('vectors')
            space:format({
                { name = 'id',     type = 'unsigned' },
                { name = 'vector', type = 'array'    },
            })
            space:create_index('primary')
        end
        if box.space.vectors.index.vector then
            box.space.vectors.index.vector:drop()
        end
    end)
end

local function after_each(cg)
    cg.server:exec(function()
        if box.space.vectors.index.vector then
            box.space.vectors.index.vector:drop()
        end
    end)
end

local function test_index_creation(cg)
    cg.server:exec(function(params)
        box.space.vectors:create_index('vector', {
            type = 'usearch',
            dimension = params.dimension,
            expansion_add = params.expansion_add,
            expansion_search = params.expansion_search,
            connectivity = params.connectivity,
            metric_kind = params.metric_kind,
        })
    end, { cg.params })
end

-- Test with different metrics:

local g = t.group("create index with different metrics", t.helpers.matrix({
	metric_kind = {
        'cos', 'ip', 'l2sq', 'haversine',
        'divergence', 'pearson', 'jaccard',
        'hamming', 'tanimoto', 'sorensen'
    }, -- 10
    dimension = { 96 },
    quantization = { 'f32' },
    expansion_add = { 64 },
    expansion_search = { 32 },
    connectivity = { 16 },
}))

g.before_all(before_all)
g.after_all(after_all)
g.before_each(before_each)
g.after_each(after_each)

g.test_index_creation = test_index_creation

g = t.group("create index with different options", t.helpers.matrix({
	metric_kind = { 'ip' },

    dimension = { 96, 1536 }, -- 2
    quantization = { 'f32', 'f64' }, -- 2
    expansion_add = { 32, 64 }, -- 2
    expansion_search = { 32, 64 }, -- 2
    connectivity = { 16, 32 }, -- 2
}))

g.before_all(before_all)
g.after_all(after_all)
g.before_each(before_each)
g.after_each(after_each)

g.test_index_creation = test_index_creation
