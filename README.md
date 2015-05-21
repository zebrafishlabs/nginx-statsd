nginx-statsd
============

An nginx module for sending statistics to statsd.

This is how to use the nginx-statsd module:

	http {

		# Set the server that you want to send stats to.
		statsd_server your.statsd.server.com;

		# Randomly sample 10% of requests so that you do not overwhelm your statsd server.
		# Defaults to sending all statsd (100%).
		statsd_sample_rate 10; # 10% of requests


		server {
			listen 80;
			server_name www.your.domain.com;

			# Increment "your_product.requests" by 1 whenever any request hits this server.
			statsd_count "your_product.requests" 1;

			# send set to statsd to count unique visitors, the metric should be a dynamic property
			# Here I use the $remote_addr variable from nginx to register the request from a unique IP
			# Argument must be a String
			statsd_set "your_unique_metric" "$remote_addr";

			location / {

				# Increment the key by 1 when this location is hit.
				statsd_count "your_product.pages.index_requests" 1;

				# Increment the key by 1, but only if $request_completion is set to something.
				statsd_count "your_product.pages.index_responses" 1 "$request_completion";

				# Send a timing to "your_product.pages.index_response_time" equal to the value
				# returned from the upstream server. If this value evaluates to 0 or empty-string,
				# it will not be sent. Thus, there is no need to add a test.
				statsd_timing "your_product.pages.index_response_time" "$upstream_response_time";

				# Increment a key based on the value of a custom header. Only sends the value if
				# the custom header exists in the upstream response.
				statsd_count "your_product.custom_$upstream_http_x_some_custom_header" 1
					"$upstream_http_x_some_custom_header";

				proxy_pass http://some.other.domain.com;
			}
		}
	}
