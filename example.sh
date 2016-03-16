go build
./directqueue --console "/home/vdmi/vhosts/vdmi/narrowscape/vendor/bin/drupal" --site "/home/vdmi/vhosts/vdmi/narrowscape/web" --default-worker-count 2 --queue-workers "narrowscape_screencapture:3" --uri "http://web.narrowscape.vdmi"
