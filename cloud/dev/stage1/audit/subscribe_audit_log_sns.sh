#!/bin/bash

aws sns subscribe --topic-arn $1 --protocol $2 --notification-endpoint $3
