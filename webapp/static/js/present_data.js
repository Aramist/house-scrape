const padding = 60;
const width = window.innerWidth;
const height = window.innerHeight;
const innerWidth = width - 2 * padding;
const innerHeight = height - 2 * padding;


var x_accessor = function(data) {
    return data['lon'];
}

var y_accessor = function(data) {
    return data['lat'];
};


var fix_url = function(link, params) {
    let url = new URL(link);
    Object.keys(params)
        .forEach(key => url.searchParams.append(key, params[key]))
    return url.toString();
};

var get_data = function(svg) {
    let parameters = {
        method: "landValue",
        lat: "25.960789",
        lon: "-80.2208063"
    };

    d3.text(fix_url('http://localhost:5000/api/v1', parameters))
        .then((res) => {
            if(res === undefined){
                console.log("Failed to get property data")
                return;
            }
            res_json = JSON.parse(res);

            console.log(res_json);

            let data_tree = d3.quadtree()
                .x(x_accessor)
                .y(y_accessor)
                .addAll(res_json);
            display_data(svg, [data_tree, res_json]);
        });
};


var get_rect = function(data_tree, bounds) {
    nodes = [];
    failed_boxes = [];
    data_tree.visit((node, x0, y0, x1, y1) => {
        if (!node.length)
            do {
                let lat = node.data['lat'], lon = node.data['lon'];
                let bx1 = bounds[0][1],
                    by1 = bounds[1][1],
                    bx0 = bounds[0][0],
                    by0 = bounds[1][0];
                if(lat <= bx1 && lat >= bx0 && lon <= by1 && lon >= by0)
                    nodes.push(node.data);
            } while (node = node.next);

        let intersection = !(
            x0 >= bounds[0][1] ||
            x1 <= bounds[0][0] ||
            y0 >= bounds[1][1] ||
            y1 <= bounds[1][0]
        );

        if(intersection)
            return false;

        return true;
    });
    return nodes;
};

var moving_avgs = function(data_tree, extent, step = 4e-4, radius = 1e-3){
    let x0 = extent[0][0],
        x1 = extent[0][1],
        y0 = extent[1][0],
        y1 = extent[1][1];

    let x_range = d3.range(x0 - radius, x1 + radius, step)
        .concat(x1 - radius);

    let y_range = d3.range(y0 - radius, y1 + radius, step)
        .concat(y1 - radius);

    let data = [];

    x_range.forEach((cx) => {
        y_range.forEach((cy) => {
            let box_bounds = [
                [cx - radius, cx + radius],
                [cy - radius, cy + radius]
            ];

            let node_list = get_rect(data_tree, box_bounds);

            node_list = node_list.filter(node  => {
                return node['land_unit'] === 'Square Ft.' &&
                node['land_value'] > 0
            });

            if(!Array.isArray(node_list) || !node_list.length)
                return;

            let cm_x = d3.mean(node_list, node => node['lat']);
            let cm_y = d3.mean(node_list, node => node['lon']);
            let mean_land = d3.mean(node_list, node => node['land_value']);

            data.push([cx, cy, mean_land]);
        });
    });

    return data;
};

var display_data = function(svg, data_tree) {
    let quadtree = data_tree[0], all_nodes = data_tree[1];

    let extent = [
        d3.extent(all_nodes, (s) => s['lat']),
        d3.extent(all_nodes, (s) => s['lon'])
    ];


    // Represents latitude
    let lat = d3.scaleLinear()
        .domain(extent[0])
        .range([0, innerWidth]);

    let lon = d3.scaleLinear()
        .domain(extent[1])
        .range([innerHeight, 0]);

    svg.selectAll('.point')
        .data(all_nodes)
        .enter()
        .append('circle')
        .attr('class', 'point')
        .attr('cx', (d) => {
            return lat(d['lat']);
        })
        .attr('cy', (d) => {
            return lon(d['lon']);
        })
        .attr('r', 2);

    centroids = moving_avgs(quadtree, extent, 8e-4, 5e-3);

    color_scale_intermediary = d3.scaleLog()
        .domain(d3.extent(centroids, d => d[2]))
        .range([0, 1]);

    color_scale = d3.interpolateLab('red', 'green')

    svg.selectAll('.centroids')
        .data(centroids)
        .enter()
        .append('circle')
        .attr('class', 'centroid')
        .attr('cx', d => lat(d[0]))
        .attr('cy', d => lon(d[1]))
        .attr('r', 2)
        .style('fill', d => color_scale(color_scale_intermediary(d[2])));
};


var make_chart = function(svg) {

};


var make_svg = function() {
    let svg = d3.select('body').append('svg')
        .attr('width', width)
        .attr('height', height);

    let chart = svg.append('g')
        .attr('transform', `translate(${padding}, ${padding})`)
        .attr('width', innerWidth)
        .attr('height', innerHeight);

    return chart;
};

d3.select(window).on('load', (event) => {
    let svg = make_svg();
    get_data(svg);
});
