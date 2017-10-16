#!/bin/sh

git clone https://github.com/yanif/fluent-plugin-cloudwatch-logs.git
cd fluent-plugin-cloudwatch-logs
gem install bundler
rake build
gem install pkg/fluent-plugin-cloudwatch-logs-0.4.1.gem
gem uninstall bundler -a -x
cd ..
rm -rf fluent-plugin-cloudwatch-logs
