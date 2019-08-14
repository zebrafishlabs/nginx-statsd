/*
 * nginx-statsd module
 * Copyright (C) 2012 Zebrafish Labs Inc.
 *
 * Much of this source code was derived from nginx-udplog-module which
 * has the following copyright. Please refer to the LICENSE file for
 * details.
 *
 * Copyright (C) 2010 Valery Kholodkov
*/
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

#define STATSD_DEFAULT_PORT 			8125

#define STATSD_TYPE_COUNTER	0x0001
#define STATSD_TYPE_TIMING  0x0002

/*
 * Max StartsD message length = 1472
 * - 1 ASCII character = 1 byte
 * - 1 UDP packet payload = 1472 bytes ( 1500-20-8 )
*/
#define STATSD_MAX_STR 1472

#define ngx_conf_merge_ptr_value(conf, prev, default)            		\
 	if (conf == NGX_CONF_UNSET_PTR) {                               	\
        conf = (prev == NGX_CONF_UNSET_PTR) ? default : prev;           \
	}

#if defined nginx_version && nginx_version >= 8021
typedef ngx_addr_t ngx_statsd_addr_t;
#else
typedef ngx_peer_addr_t ngx_statsd_addr_t;
#endif

typedef struct {
    ngx_statsd_addr_t         peer_addr;
    ngx_resolver_connection_t      *udp_connection;
    ngx_log_t                 *log;
} ngx_udp_endpoint_t;

typedef struct {
	ngx_array_t                *endpoints;
} ngx_http_statsd_main_conf_t;

typedef struct {
	ngx_uint_t			   	    type;

	ngx_str_t			   		key;
	ngx_uint_t			   		metric;
	ngx_flag_t					valid;

	ngx_http_complex_value_t 	*ckey;
	ngx_http_complex_value_t 	*cmetric;
	ngx_http_complex_value_t	*cvalid;
} ngx_statsd_stat_t;

typedef struct {
    int	                    off;
    ngx_udp_endpoint_t      *endpoint;
	ngx_uint_t				sample_rate;
	ngx_array_t				*stats;
} ngx_http_statsd_conf_t;


static void ngx_statsd_updater_cleanup(void *data);
static ngx_int_t ngx_http_statsd_udp_send(ngx_udp_endpoint_t *l, u_char *buf, size_t len);

static void *ngx_http_statsd_create_main_conf(ngx_conf_t *cf);
static void *ngx_http_statsd_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_statsd_merge_loc_conf(ngx_conf_t *cf, void *parent,
    void *child);

static char *ngx_http_statsd_set_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_statsd_add_stat(ngx_conf_t *cf, ngx_command_t *cmd, void *conf, ngx_uint_t type);
static char *ngx_http_statsd_add_count(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_statsd_add_timing(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_str_t ngx_http_statsd_key_get_value(ngx_http_request_t *r, ngx_http_complex_value_t *cv, ngx_str_t v);
static ngx_str_t ngx_http_statsd_key_value(ngx_str_t *str);
static ngx_uint_t ngx_http_statsd_metric_get_value(ngx_http_request_t *r, ngx_http_complex_value_t *cv, ngx_uint_t v);
static ngx_uint_t ngx_http_statsd_metric_value(ngx_str_t *str);
static ngx_flag_t ngx_http_statsd_valid_get_value(ngx_http_request_t *r, ngx_http_complex_value_t *cv, ngx_flag_t v);
static ngx_flag_t ngx_http_statsd_valid_value(ngx_str_t *str);

uintptr_t ngx_escape_statsd_key(u_char *dst, u_char *src, size_t size);

static ngx_int_t ngx_http_statsd_init(ngx_conf_t *cf);

static ngx_command_t  ngx_http_statsd_commands[] = {

	{ ngx_string("statsd_server"),
	  NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	  ngx_http_statsd_set_server,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  0,
	  NULL },

	{ ngx_string("statsd_sample_rate"),
	  NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	  ngx_conf_set_num_slot,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  offsetof(ngx_http_statsd_conf_t, sample_rate),
	  NULL },

	{ ngx_string("statsd_count"),
	  NGX_HTTP_SRV_CONF|NGX_HTTP_SIF_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE23,
	  ngx_http_statsd_add_count,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  0,
	  NULL },

	{ ngx_string("statsd_timing"),
	  NGX_HTTP_SRV_CONF|NGX_HTTP_SIF_CONF|NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE23,
	  ngx_http_statsd_add_timing,
	  NGX_HTTP_LOC_CONF_OFFSET,
	  0,
	  NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_statsd_module_ctx = {
    NULL,                                  /* preconfiguration */
    ngx_http_statsd_init,                  /* postconfiguration */

    ngx_http_statsd_create_main_conf,      /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_statsd_create_loc_conf,       /* create location configration */
    ngx_http_statsd_merge_loc_conf         /* merge location configration */
};


ngx_module_t  ngx_http_statsd_module = {
    NGX_MODULE_V1,
    &ngx_http_statsd_module_ctx,           /* module context */
    ngx_http_statsd_commands,              /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

static ngx_str_t
ngx_http_statsd_key_get_value(ngx_http_request_t *r, ngx_http_complex_value_t *cv, ngx_str_t v)
{
	ngx_str_t val;
	if (cv == NULL) {
		return v;
	}

	if (ngx_http_complex_value(r, cv, &val) != NGX_OK) {
		return (ngx_str_t) ngx_null_string;
	};

	return ngx_http_statsd_key_value(&val);
};

static ngx_str_t
ngx_http_statsd_key_value(ngx_str_t *value)
{
	return *value;
};

static ngx_uint_t
ngx_http_statsd_metric_get_value(ngx_http_request_t *r, ngx_http_complex_value_t *cv, ngx_uint_t v)
{
	ngx_str_t val;
	if (cv == NULL) {
		return v;
	}

	if (ngx_http_complex_value(r, cv, &val) != NGX_OK) {
		return 0;
	};

	return ngx_http_statsd_metric_value(&val);
};

static ngx_uint_t
ngx_http_statsd_metric_value(ngx_str_t *value)
{
	ngx_int_t n, m;

    if (value->len == 1 && value->data[0] == '-') {
    	return (ngx_uint_t) -1;
	};

	/* Hack to convert milliseconds to a number. */
	if (value->len > 4 && value->data[value->len - 4] == '.') {
		n = ngx_atoi(value->data, value->len - 4);
		m = ngx_atoi(value->data + (value->len - 3), 3);
		return (ngx_uint_t) ((n * 1000) + m);

	} else {
		n = ngx_atoi(value->data, value->len);
		if (n > 0) {
			return (ngx_uint_t) n;
		};
	};

	return 0;
};

static ngx_flag_t
ngx_http_statsd_valid_get_value(ngx_http_request_t *r, ngx_http_complex_value_t *cv, ngx_flag_t v)
{
	ngx_str_t val;
	if (cv == NULL) {
		return v;
	}

	if (ngx_http_complex_value(r, cv, &val) != NGX_OK) {
		return 0;
	};

	return ngx_http_statsd_valid_value(&val);
};

static ngx_flag_t
ngx_http_statsd_valid_value(ngx_str_t *value)
{
	return (ngx_flag_t) (value->len > 0 ? 1 : 0);
};

ngx_int_t
ngx_http_statsd_handler(ngx_http_request_t *r)
{
    u_char                    line[STATSD_MAX_STR], *p;
    const char *              metric_type;
    ngx_http_statsd_conf_t   *ulcf;
	ngx_statsd_stat_t 		 *stats;
	ngx_statsd_stat_t		  stat;
	ngx_uint_t 			      c;
	ngx_uint_t				  n;
	ngx_str_t				  s;
	ngx_flag_t				  b;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "http statsd handler");

    ulcf = ngx_http_get_module_loc_conf(r, ngx_http_statsd_module);

    if (ulcf->off == 1 || ulcf->endpoint == NULL) {
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "statsd: handler off");
        return NGX_OK;
    }

	// Use a random distribution to sample at sample rate.
	if (ulcf->sample_rate < 100 && (uint) (ngx_random() % 100) >= ulcf->sample_rate) {
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "statsd: skipping sample");
		return NGX_OK;
	}

	stats = ulcf->stats->elts;
	for (c = 0; c < ulcf->stats->nelts; c++) {

		stat = stats[c];
		s = ngx_http_statsd_key_get_value(r, stat.ckey, stat.key);
		ngx_escape_statsd_key(s.data, s.data, s.len);

		n = ngx_http_statsd_metric_get_value(r, stat.cmetric, stat.metric);
		b = ngx_http_statsd_valid_get_value(r, stat.cvalid, stat.valid);

		if (b == 0 || s.len == 0 || n <= 0) {
			// Do not log if not valid, key is invalid, or valud is lte 0.
			ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "statsd: no value to send");
         	continue;
		};

		if (stat.type == STATSD_TYPE_COUNTER) {
			metric_type = "c";
		} else if (stat.type == STATSD_TYPE_TIMING) {
			metric_type = "ms";
		} else {
			metric_type = NULL;
		}

		if (metric_type) {
			if (ulcf->sample_rate < 100) {
				p = ngx_snprintf(line, STATSD_MAX_STR, "%V:%d|%s|@0.%02d", &s, n, metric_type, ulcf->sample_rate);
			} else {
				p = ngx_snprintf(line, STATSD_MAX_STR, "%V:%d|%s", &s, n, metric_type);
			}
			ngx_http_statsd_udp_send(ulcf->endpoint, line, p - line);
		}
	}

    return NGX_OK;
}

static ngx_int_t ngx_statsd_init_endpoint(ngx_conf_t *cf, ngx_udp_endpoint_t *endpoint) {
    ngx_pool_cleanup_t    *cln;
    ngx_resolver_connection_t  *rec;

	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0,
			   "statsd: initting endpoint");

    cln = ngx_pool_cleanup_add(cf->pool, 0);
    if(cln == NULL) {
        return NGX_ERROR;
    }

    cln->handler = ngx_statsd_updater_cleanup;
    cln->data = endpoint;

    rec = ngx_calloc(sizeof(ngx_resolver_connection_t), cf->log);
    if (rec == NULL) {
        return NGX_ERROR;
    }

    endpoint->udp_connection = rec;

    rec->sockaddr = endpoint->peer_addr.sockaddr;
    rec->socklen = endpoint->peer_addr.socklen;
    rec->server = endpoint->peer_addr.name;

    endpoint->log = &cf->cycle->new_log;

    return NGX_OK;
}

static void
ngx_statsd_updater_cleanup(void *data)
{
    ngx_udp_endpoint_t  *e = data;

    ngx_log_debug0(NGX_LOG_DEBUG_CORE, ngx_cycle->log, 0,
                   "cleanup statsd_updater");

    if(e->udp_connection) {
        if(e->udp_connection->udp) {
            ngx_close_connection(e->udp_connection->udp);
        }

        ngx_free(e->udp_connection);
    }
}

static void ngx_http_statsd_udp_dummy_handler(ngx_event_t *ev)
{
}

static ngx_int_t
ngx_http_statsd_udp_connect(ngx_resolver_connection_t *rec)
{
    int                rc;
    ngx_int_t          event;
    ngx_event_t       *rev, *wev;
    ngx_socket_t       s;
    ngx_connection_t  *c;

    s = ngx_socket(rec->sockaddr->sa_family, SOCK_DGRAM, 0);

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, &rec->log, 0, "UDP socket %d", s);

    if (s == (ngx_socket_t) -1) {
        ngx_log_error(NGX_LOG_ALERT, &rec->log, ngx_socket_errno,
                      ngx_socket_n " failed");
        return NGX_ERROR;
    }

    c = ngx_get_connection(s, &rec->log);

    if (c == NULL) {
        if (ngx_close_socket(s) == -1) {
            ngx_log_error(NGX_LOG_ALERT, &rec->log, ngx_socket_errno,
                          ngx_close_socket_n "failed");
        }

        return NGX_ERROR;
    }

    if (ngx_nonblocking(s) == -1) {
        ngx_log_error(NGX_LOG_ALERT, &rec->log, ngx_socket_errno,
                      ngx_nonblocking_n " failed");

        goto failed;
    }

    rev = c->read;
    wev = c->write;

    rev->log = &rec->log;
    wev->log = &rec->log;

    rec->udp = c;

    c->number = ngx_atomic_fetch_add(ngx_connection_counter, 1);

    ngx_log_debug3(NGX_LOG_DEBUG_EVENT, &rec->log, 0,
                   "connect to %V, fd:%d #%uA", &rec->server, s, c->number);

    rc = connect(s, rec->sockaddr, rec->socklen);

    /* TODO: iocp */

    if (rc == -1) {
        ngx_log_error(NGX_LOG_CRIT, &rec->log, ngx_socket_errno,
                      "connect() failed");

        goto failed;
    }

    /* UDP sockets are always ready to write */
    wev->ready = 1;

    event = (ngx_event_flags & NGX_USE_CLEAR_EVENT) ?
                /* kqueue, epoll */                 NGX_CLEAR_EVENT:
                /* select, poll, /dev/poll */       NGX_LEVEL_EVENT;
                /* eventport event type has no meaning: oneshot only */

    if (ngx_add_event(rev, NGX_READ_EVENT, event) != NGX_OK) {
        goto failed;
    }

    return NGX_OK;

failed:

    ngx_close_connection(c);
    rec->udp = NULL;

    return NGX_ERROR;
}

static ngx_int_t
ngx_http_statsd_udp_send(ngx_udp_endpoint_t *l, u_char *buf, size_t len)
{
    ssize_t                n;
    ngx_resolver_connection_t  *rec;

    rec = l->udp_connection;
    if (rec->udp == NULL) {

        rec->log = *l->log;
        rec->log.handler = NULL;
        rec->log.data = NULL;
        rec->log.action = "logging";

        if(ngx_http_statsd_udp_connect(rec) != NGX_OK) {
            if(rec->udp != NULL) {
                ngx_free_connection(rec->udp);
                rec->udp = NULL;
            }

            return NGX_ERROR;
        }

        rec->udp->data = l;
        rec->udp->read->handler = ngx_http_statsd_udp_dummy_handler;
        rec->udp->read->resolver = 0;
    }

    n = ngx_send(rec->udp, buf, len);

    if (n == -1) {
        return NGX_ERROR;
    }

    if ((size_t) n != (size_t) len) {
#if defined nginx_version && nginx_version >= 8032
        ngx_log_error(NGX_LOG_CRIT, &rec->log, 0, "send() incomplete");
#else
        ngx_log_error(NGX_LOG_CRIT, rec->log, 0, "send() incomplete");
#endif
        return NGX_ERROR;
    }

    return NGX_OK;
}

static void *
ngx_http_statsd_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_statsd_main_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_statsd_main_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    return conf;
}

static void *
ngx_http_statsd_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_statsd_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_statsd_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }
	conf->endpoint = NGX_CONF_UNSET_PTR;
    conf->off = NGX_CONF_UNSET;
	conf->sample_rate = NGX_CONF_UNSET_UINT;
	conf->stats = NULL;

    return conf;
}

static char *
ngx_http_statsd_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_statsd_conf_t *prev = parent;
    ngx_http_statsd_conf_t *conf = child;
	ngx_statsd_stat_t *stat;
	ngx_statsd_stat_t prev_stat;
	ngx_statsd_stat_t 		*prev_stats;
	ngx_uint_t				i;
	ngx_uint_t				sz;

	ngx_conf_merge_ptr_value(conf->endpoint, prev->endpoint, NULL);
	ngx_conf_merge_off_value(conf->off, prev->off, 1);
	ngx_conf_merge_uint_value(conf->sample_rate, prev->sample_rate, 100);

	if (conf->stats == NULL) {
		sz = (prev->stats != NULL ? prev->stats->nelts : 2);
		conf->stats = ngx_array_create(cf->pool, sz, sizeof(ngx_statsd_stat_t));
		if (conf->stats == NULL) {
        	return NGX_CONF_ERROR;
		}
	}
	if (prev->stats != NULL) {
		prev_stats = prev->stats->elts;
		for (i = 0; i < prev->stats->nelts; i++) {
			stat = ngx_array_push(conf->stats);
			ngx_memzero(stat, sizeof(ngx_statsd_stat_t));

			prev_stat = prev_stats[i];

			stat->type = prev_stat.type;
			stat->key = prev_stat.key;
			stat->metric = prev_stat.metric;
			stat->ckey = prev_stat.ckey;
			stat->cmetric = prev_stat.cmetric;
			stat->valid = prev_stat.valid;
			stat->cvalid = prev_stat.cvalid;
		};
	};

    return NGX_CONF_OK;
}

static ngx_udp_endpoint_t *
ngx_http_statsd_add_endpoint(ngx_conf_t *cf, ngx_statsd_addr_t *peer_addr)
{
    ngx_http_statsd_main_conf_t    *umcf;
    ngx_udp_endpoint_t             *endpoint;
    ngx_str_t server_name;
    ngx_log_t *log = cf->log;

    umcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_statsd_module);

    if(umcf->endpoints == NULL) {
        umcf->endpoints = ngx_array_create(cf->pool, 2, sizeof(ngx_udp_endpoint_t));
        if (umcf->endpoints == NULL) {
            return NULL;
        }
    }

    endpoint = ngx_array_push(umcf->endpoints);
    if (endpoint == NULL) {
        return NULL;
    }

    endpoint->peer_addr = *peer_addr;
    
    server_name.len = endpoint->peer_addr.name.len;
    server_name.data = ngx_calloc(server_name.len, log);
    if(server_name.data == NULL) {
        ngx_log_error(NGX_LOG_ERR, log, 0, "%s: memory allocation failure", __func__);
        return NULL;
    }
    ngx_memcpy(server_name.data, endpoint->peer_addr.name.data, server_name.len);
    endpoint->peer_addr.name = server_name;

    return endpoint;
}

static char *
ngx_http_statsd_set_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_statsd_conf_t      *ulcf = conf;
    ngx_str_t                   *value;
    ngx_url_t                    u;

    value = cf->args->elts;

    if (ngx_strcmp(value[1].data, "off") == 0) {
        ulcf->off = 1;
        return NGX_CONF_OK;
    }
    ulcf->off = 0;

    ngx_memzero(&u, sizeof(ngx_url_t));

    u.url = value[1];
    u.default_port = STATSD_DEFAULT_PORT;
    u.no_resolve = 0;

    if(ngx_parse_url(cf->pool, &u) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "%V: %s", &u.host, u.err);
        return NGX_CONF_ERROR;
    }

	ulcf->endpoint = ngx_http_statsd_add_endpoint(cf, &u.addrs[0]);
    if(ulcf->endpoint == NULL) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

ngx_int_t
ngx_cntm_set_statsd_server(ngx_url_t *target)
{
    ngx_http_statsd_main_conf_t    *umcf;
    ngx_udp_endpoint_t *endpoint;
    ngx_str_t server_name;
    ngx_log_t *log = ngx_cycle->log;

    umcf = ngx_http_cycle_get_module_main_conf(ngx_cycle, ngx_http_statsd_module);
    if (umcf == NULL || umcf->endpoints == NULL) {
        return NGX_OK;
    }
    
    endpoint = (ngx_udp_endpoint_t *)umcf->endpoints->elts;
   
    if(ngx_memcmp(endpoint->peer_addr.sockaddr, &target->sockaddr, sizeof(struct sockaddr_in)) == 0) {
        // No change in StatsD address
        return NGX_OK;
    }

    if (endpoint->peer_addr.name.len != target->addrs[0].name.len) {
        server_name.len = target->addrs[0].name.len;
        server_name.data = ngx_calloc(server_name.len, log);
        if(server_name.data == NULL) {
            ngx_log_error(NGX_LOG_ERR, log, 0, "%s: Failed to update StatsD server info", __func__);
            return NGX_ERROR;
        }
        ngx_free(endpoint->peer_addr.name.data);
        endpoint->peer_addr.name = server_name;
    }
    ngx_memcpy(endpoint->peer_addr.sockaddr, &target->sockaddr, sizeof(struct sockaddr_in));
    ngx_memcpy(endpoint->peer_addr.name.data, target->addrs[0].name.data, endpoint->peer_addr.name.len);
    
    if(endpoint->udp_connection) {
        endpoint->udp_connection->server = endpoint->peer_addr.name;
        if(endpoint->udp_connection->udp) {
            ngx_close_connection(endpoint->udp_connection->udp);
            endpoint->udp_connection->udp = NULL;
        }
    }

    return NGX_OK;
}

static char *
ngx_http_statsd_add_stat(ngx_conf_t *cf, ngx_command_t *cmd, void *conf, ngx_uint_t type) {
    ngx_http_statsd_conf_t      		*ulcf = conf;
	ngx_http_complex_value_t			key_cv;
	ngx_http_compile_complex_value_t    key_ccv;
	ngx_http_complex_value_t			metric_cv;
	ngx_http_compile_complex_value_t    metric_ccv;
	ngx_http_complex_value_t			valid_cv;
	ngx_http_compile_complex_value_t    valid_ccv;
    ngx_str_t                   		*value;
	ngx_statsd_stat_t 					*stat;
	ngx_int_t							n;
	ngx_str_t							s;
	ngx_flag_t							b;

    value = cf->args->elts;

	if (ulcf->stats == NULL) {
		ulcf->stats = ngx_array_create(cf->pool, 10, sizeof(ngx_statsd_stat_t));
		if (ulcf->stats == NULL) {
        	return NGX_CONF_ERROR;
		}
	}

	stat = ngx_array_push(ulcf->stats);
	if (stat == NULL) {
    	return NGX_CONF_ERROR;
	}

	ngx_memzero(stat, sizeof(ngx_statsd_stat_t));

	stat->type = type;
	stat->valid = 1;

	ngx_memzero(&key_ccv, sizeof(ngx_http_compile_complex_value_t));
	key_ccv.cf = cf;
	key_ccv.value = &value[1];
	key_ccv.complex_value = &key_cv;

	if (ngx_http_compile_complex_value(&key_ccv) != NGX_OK) {
		return NGX_CONF_ERROR;
	}

	if (key_cv.lengths == NULL) {
		s = ngx_http_statsd_key_value(&value[1]);
		/*if (n < 0) {
			ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "invalid parameter \"%V\"", &value[2]);
			return NGX_CONF_ERROR;
		};*/
		stat->key = (ngx_str_t) s;
	} else {
		stat->ckey = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
		if (stat->ckey == NULL) {
			return NGX_CONF_ERROR;
		}
		*stat->ckey = key_cv;
	}

	ngx_memzero(&metric_ccv, sizeof(ngx_http_compile_complex_value_t));
	metric_ccv.cf = cf;
	metric_ccv.value = &value[2];
	metric_ccv.complex_value = &metric_cv;

	if (ngx_http_compile_complex_value(&metric_ccv) != NGX_OK) {
		return NGX_CONF_ERROR;
	}

	if (metric_cv.lengths == NULL) {
		n = ngx_http_statsd_metric_value(&value[2]);
		if (n < 0) {
			ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "invalid parameter \"%V\"", &value[2]);
			return NGX_CONF_ERROR;
		};
		stat->metric = (ngx_uint_t) n;
	} else {
		stat->cmetric = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
		if (stat->cmetric == NULL) {
			return NGX_CONF_ERROR;
		}
		*stat->cmetric = metric_cv;
	}

	if (cf->args->nelts > 3) {
		ngx_memzero(&valid_ccv, sizeof(ngx_http_compile_complex_value_t));
		valid_ccv.cf = cf;
		valid_ccv.value = &value[3];
		valid_ccv.complex_value = &valid_cv;

		if (ngx_http_compile_complex_value(&valid_ccv) != NGX_OK) {
			return NGX_CONF_ERROR;
		}

		if (valid_cv.lengths == NULL) {
			b = ngx_http_statsd_valid_value(&value[3]);
			if (b < 0) {
				ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "invalid parameter \"%V\"", &value[3]);
				return NGX_CONF_ERROR;
			};
			stat->valid = (ngx_flag_t) b;
		} else {
			stat->cvalid = ngx_palloc(cf->pool, sizeof(ngx_http_complex_value_t));
			if (stat->cvalid == NULL) {
				return NGX_CONF_ERROR;
			}
			*stat->cvalid = valid_cv;
		}
	}

	return NGX_CONF_OK;
}

static char *
ngx_http_statsd_add_count(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	return ngx_http_statsd_add_stat(cf, cmd, conf, STATSD_TYPE_COUNTER);
}

static char *
ngx_http_statsd_add_timing(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	return ngx_http_statsd_add_stat(cf, cmd, conf, STATSD_TYPE_TIMING);
}

static ngx_int_t
ngx_http_statsd_init(ngx_conf_t *cf)
{
    ngx_int_t                     rc;
    ngx_uint_t                    i;
    ngx_http_core_main_conf_t    *cmcf;
    ngx_http_statsd_main_conf_t  *umcf;
    ngx_http_handler_pt          *h;
    ngx_udp_endpoint_t           *e;

    umcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_statsd_module);

    if(umcf->endpoints != NULL) {
        e = umcf->endpoints->elts;
        for(i = 0;i < umcf->endpoints->nelts;i++) {
            rc = ngx_statsd_init_endpoint(cf, e + i);

            if(rc != NGX_OK) {
                return NGX_ERROR;
            }
        }

        cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

        h = ngx_array_push(&cmcf->phases[NGX_HTTP_LOG_PHASE].handlers);
        if (h == NULL) {
            return NGX_ERROR;
        }

        *h = ngx_http_statsd_handler;
    }

    return NGX_OK;
}

uintptr_t
ngx_escape_statsd_key(u_char *dst, u_char *src, size_t size)
{
    ngx_uint_t      n;
    uint32_t       *escape;

                    /* " ", "#", """, "%", "'", %00-%1F, %7F-%FF */

    static uint32_t   statsd_key[] = {
        0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */

                    /* ?>=< ;:98 7654 3210  /.-, +*)( '&%$ #"!  */
		0xfc009fff, /* 1111 1100 0000 0000  1001 1111 1111 1111 */

                    /* _^]\ [ZYX WVUT SRQP  ONML KJIH GFED CBA@ */
		0x78000001, /* 0111 1000 0000 0000  0000 0000 0000 0001 */

                    /*  ~}| {zyx wvut srqp  onml kjih gfed cba` */
		0xf8000001, /* 1111 1000 0000 0000  0000 0000 0000 0001 */

        0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */
        0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */
        0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */
        0xffffffff  /* 1111 1111 1111 1111  1111 1111 1111 1111 */
    };

    static uint32_t  *map[] =
        { statsd_key };


    escape = map[0];

    if (dst == NULL) {

        /* find the number of the characters to be escaped */

        n = 0;

        while (size) {
            if (escape[*src >> 5] & (1 << (*src & 0x1f))) {
                n++;
            }
            src++;
            size--;
        }

        return (uintptr_t) n;
    }

    while (size) {
        if (escape[*src >> 5] & (1 << (*src & 0x1f))) {
            *dst++ = '_';
            src++;

        } else {
            *dst++ = *src++;
        }
        size--;
    }

    return (uintptr_t) dst;
}