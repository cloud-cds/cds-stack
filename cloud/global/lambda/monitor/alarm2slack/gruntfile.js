module.exports = function(grunt) {
  grunt.loadNpmTasks('grunt-aws-lambda');
  grunt.loadNpmTasks('grunt-jscs');
  grunt.initConfig({
    jscs: {
      src: 'src/*.js',
      options: {
        config: '.jscsrc',
        esnext: false,
        verbose: true,
        requireCurlyBraces: [],
      },
    },
    lambda_invoke: {
      default: {
        options: {
        },
      },
    },
    lambda_package: {
      default: {
        options: {},
      },
    }
  });

  grunt.registerTask('check', ['jscs']);

  grunt.registerTask('run', ['check', 'lambda_invoke']);
  grunt.registerTask('run-nochecks', ['lambda_invoke']);

  grunt.registerTask('build-nochecks', ['lambda_package']);
  grunt.registerTask('build', ['check', 'build-nochecks']);

  grunt.registerTask('package', ['lambda_package']);
};