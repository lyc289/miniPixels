LOAD -o /home/liuyichen/workspace/miniPixels-Project/data/long -t /home/liuyichen/workspace/miniPixels-Project/data/output -s struct<a:int,b:int> -n 10 -r \|

LOAD -o /home/liuyichen/workspace/miniPixels-Project/data/test_decimal -t /home/liuyichen/workspace/miniPixels-Project/data/output -s struct<a:decimal(15,2)> -n 100 -r \|

LOAD -o /home/liuyichen/workspace/miniPixels-Project/data/testtimestamp -t /home/liuyichen/workspace/miniPixels-Project/data/output -s struct<a:timestamp> -n 100 -r \|

LOAD -o /home/liuyichen/workspace/miniPixels-Project/data/testdate -t /home/liuyichen/workspace/miniPixels-Project/data/output -s struct<a:date> -n 100 -r \|
