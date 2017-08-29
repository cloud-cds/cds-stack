var path = require('path');
var webpack = require("webpack");

module.exports = {
  resolve: {
    modules: [
      path.resolve('./js'),
      path.resolve('./bower_components'),
      path.resolve('./bower_components/jquery/jquerydist'),
      path.resolve('./bower_components/jquery-ui'),
      path.resolve('./bower_components/Flot')
    ]
  },
  entry: {
    app: ['constants.js',
          'main.js',
          'modernizr.js',
          'jquery.min.js',
          'jquery-ui.min.js',
          'jquery.flot.js',
          'jquery.flot.time.js',
          'jquery.flot.selection.js',
          'jquery.flot.threshold.js',
          'jquery.flot.crosshair.js',
          'jquery.flot.navigate.js'
    ],
  },
  plugins: [
      new webpack.ProvidePlugin({
         $: "jquery",
         jQuery: "jquery"
     })
  ],
  output: {
    filename: '[name].bundle.js',
    path: path.resolve(__dirname, 'dist')
  }
};
