package graphite_test

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/services/graphite"
)

func BenchmarkParse(b *testing.B) {
	p, err := graphite.NewParser([]string{
		"*.* .wrong.measurement*",
		"servers.* .host.measurement*",
		"servers.localhost .host.measurement*",
		"*.localhost .host.measurement*",
		"*.*.cpu .host.measurement*",
		"a.b.c .host.measurement*",
		"influxd.*.foo .host.measurement*",
		"prod.*.mem .host.measurement*",
	}, nil)

	if err != nil {
		b.Fatalf("unexpected error creating parser, got %v", err)
	}

	for i := 0; i < b.N; i++ {
		p.Parse("servers.localhost.cpu.load 11 1435077219")
	}
}

func TestTemplateApply(t *testing.T) {
	var tests = []struct {
		test        string
		input       string
		template    string
		measurement string
		tags        map[string]string
		err         string
	}{
		{
			test:        "metric only",
			input:       "cpu",
			template:    "measurement",
			measurement: "cpu",
		},
		{
			test:        "metric with single series",
			input:       "cpu.server01",
			template:    "measurement.hostname",
			measurement: "cpu",
			tags:        map[string]string{"hostname": "server01"},
		},
		{
			test:        "metric with multiple series",
			input:       "cpu.us-west.server01",
			template:    "measurement.region.hostname",
			measurement: "cpu",
			tags:        map[string]string{"hostname": "server01", "region": "us-west"},
		},
		{
			test: "no metric",
			tags: make(map[string]string),
			err:  `no measurement specified for template. ""`,
		},
		{
			test:        "ignore unnamed",
			input:       "foo.cpu",
			template:    "measurement",
			measurement: "foo",
			tags:        make(map[string]string),
		},
		{
			test:        "name shorter than template",
			input:       "foo",
			template:    "measurement.A.B.C",
			measurement: "foo",
			tags:        make(map[string]string),
		},
		{
			test:        "wildcard measurement at end",
			input:       "prod.us-west.server01.cpu.load",
			template:    "env.zone.host.measurement*",
			measurement: "cpu.load",
			tags:        map[string]string{"env": "prod", "zone": "us-west", "host": "server01"},
		},
		{
			test:        "skip fields",
			input:       "ignore.us-west.ignore-this-too.cpu.load",
			template:    ".zone..measurement*",
			measurement: "cpu.load",
			tags:        map[string]string{"zone": "us-west"},
		},
	}

	for _, test := range tests {
		tmpl, err := graphite.NewTemplate(test.template, nil, graphite.DefaultSeparator)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}

		measurement, tags := tmpl.Apply(test.input)
		if measurement != test.measurement {
			t.Fatalf("name parse failer.  expected %v, got %v", test.measurement, measurement)
		}
		if len(tags) != len(test.tags) {
			t.Fatalf("unexpected number of tags.  expected %v, got %v", test.tags, tags)
		}
		for k, v := range test.tags {
			if tags[k] != v {
				t.Fatalf("unexpected tag value for tags[%s].  expected %q, got %q", k, v, tags[k])
			}
		}
	}
}

func TestParseMissingMeasurement(t *testing.T) {
	_, err := graphite.NewParser([]string{"a.b.c"}, nil)
	if err == nil {
		t.Fatalf("expected error creating parser, got nil")
	}
}

func TestParse(t *testing.T) {
	testTime := time.Now().Round(time.Second)
	epochTime := testTime.Unix()
	strTime := strconv.FormatInt(epochTime, 10)

	var tests = []struct {
		test        string
		input       string
		measurement string
		tags        map[string]string
		value       float64
		time        time.Time
		template    string
		err         string
	}{
		{
			test:        "normal case",
			input:       `cpu.foo.bar 50 ` + strTime,
			template:    "measurement.foo.bar",
			measurement: "cpu",
			tags: map[string]string{
				"foo": "foo",
				"bar": "bar",
			},
			value: 50,
			time:  testTime,
		},
		{
			test:        "metric only with float value",
			input:       `cpu 50.554 ` + strTime,
			measurement: "cpu",
			template:    "measurement",
			value:       50.554,
			time:        testTime,
		},
		{
			test:     "missing metric",
			input:    `1419972457825`,
			template: "measurement",
			err:      `received "1419972457825" which doesn't have required fields`,
		},
		{
			test:     "should error parsing invalid float",
			input:    `cpu 50.554z 1419972457825`,
			template: "measurement",
			err:      `field "cpu" value: strconv.ParseFloat: parsing "50.554z": invalid syntax`,
		},
		{
			test:     "should error parsing invalid int",
			input:    `cpu 50z 1419972457825`,
			template: "measurement",
			err:      `field "cpu" value: strconv.ParseFloat: parsing "50z": invalid syntax`,
		},
		{
			test:     "should error parsing invalid time",
			input:    `cpu 50.554 14199724z57825`,
			template: "measurement",
			err:      `field "cpu" time: strconv.ParseFloat: parsing "14199724z57825": invalid syntax`,
		},
	}

	for _, test := range tests {
		p, err := graphite.NewParser([]string{test.template}, nil)
		if err != nil {
			t.Fatalf("unexpected error creating graphite parser: %v", err)
		}

		point, err := p.Parse(test.input)
		if errstr(err) != test.err {
			t.Fatalf("err does not match.  expected %v, got %v", test.err, err)
		}
		if err != nil {
			// If we erred out,it was intended and the following tests won't work
			continue
		}
		if point.Name() != test.measurement {
			t.Fatalf("name parse failer.  expected %v, got %v", test.measurement, point.Name())
		}
		if len(point.Tags()) != len(test.tags) {
			t.Fatalf("tags len mismatch.  expected %d, got %d", len(test.tags), len(point.Tags()))
		}
		f := point.Fields()["value"].(float64)
		if point.Fields()["value"] != f {
			t.Fatalf("floatValue value mismatch.  expected %v, got %v", test.value, f)
		}
		if point.Time().UnixNano()/1000000 != test.time.UnixNano()/1000000 {
			t.Fatalf("time value mismatch.  expected %v, got %v", test.time.UnixNano(), point.Time().UnixNano())
		}
	}
}

func TestParseNaN(t *testing.T) {
	p, err := graphite.NewParser([]string{"measurement*"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	pt, err := p.Parse("servers.localhost.cpu_load NaN 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	exp := models.NewPoint("servers.localhost.cpu_load",
		models.Tags{},
		models.Fields{"value": math.NaN()},
		time.Unix(1435077219, 0))

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}

	if !math.IsNaN(pt.Fields()["value"].(float64)) {
		t.Errorf("parse value mismatch: expected NaN")
	}
}

func TestFilterMatchDefault(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost .host.measurement*"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("miss.servers.localhost.cpu_load",
		models.Tags{},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("miss.servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchMultipleMeasurement(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost .host.measurement.measurement*"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu.cpu_load.10",
		models.Tags{"host": "localhost"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu.cpu_load.10 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchMultipleMeasurementSeparator(t *testing.T) {
	p, err := graphite.NewParserWithOptions(graphite.Options{
		Templates: []string{"servers.localhost .host.measurement.measurement*"},
		Separator: "_",
	})
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_cpu_load_10",
		models.Tags{"host": "localhost"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu.cpu_load.10 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchSingle(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost .host.measurement*"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestParseNoMatch(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.*.cpu .host.measurement.cpu.measurement"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("servers.localhost.memory.VmallocChunk",
		models.Tags{},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.memory.VmallocChunk 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchWildcard(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.* .host.measurement*"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchExactBeforeWildcard(t *testing.T) {
	p, err := graphite.NewParser([]string{
		"servers.* .wrong.measurement*",
		"servers.localhost .host.measurement*"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchMostLongestFilter(t *testing.T) {
	p, err := graphite.NewParser([]string{
		"*.* .wrong.measurement*",
		"servers.* .wrong.measurement*",
		"servers.localhost .wrong.measurement*",
		"servers.localhost.cpu .host.resource.measurement*", // should match this
		"*.localhost .wrong.measurement*",
	}, nil)

	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost", "resource": "cpu"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchMultipleWildcards(t *testing.T) {
	p, err := graphite.NewParser([]string{
		"*.* .wrong.measurement*",
		"servers.* .host.measurement*", // should match this
		"servers.localhost .wrong.measurement*",
		"*.localhost .wrong.measurement*",
	}, nil)

	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "server01"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.server01.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestParseDefaultTags(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost .host.measurement*"}, models.Tags{
		"region": "us-east",
		"zone":   "1c",
		"host":   "should not set",
	})
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost", "region": "us-east", "zone": "1c"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestParseDefaultTemplateTags(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost .host.measurement* zone=1c"}, models.Tags{
		"region": "us-east",
		"host":   "should not set",
	})
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost", "region": "us-east", "zone": "1c"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestParseDefaultTemplateTagsOverridGlobal(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost .host.measurement* zone=1c,region=us-east"}, models.Tags{
		"region": "shot not be set",
		"host":   "should not set",
	})
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost", "region": "us-east", "zone": "1c"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestParseTemplateWhitespace(t *testing.T) {
	p, err := graphite.NewParser([]string{"servers.localhost        .host.measurement*           zone=1c"}, models.Tags{
		"region": "us-east",
		"host":   "should not set",
	})
	if err != nil {
		t.Fatalf("unexpected error creating parser, got %v", err)
	}

	exp := models.NewPoint("cpu_load",
		models.Tags{"host": "localhost", "region": "us-east", "zone": "1c"},
		models.Fields{"value": float64(11)},
		time.Unix(1435077219, 0))

	pt, err := p.Parse("servers.localhost.cpu_load 11 1435077219")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if exp.String() != pt.String() {
		t.Errorf("parse mismatch: got %v, exp %v", pt.String(), exp.String())
	}
}

func TestFilterMatchEnquos(t *testing.T) {

        // Before testing sync the templates list with the latest production
        // configuration.
        templates := []string{
            "systems.*.cpu.* .host.measurement.cpu.measurement category=system",
            "systems.*.diskspace.* .host.measurement.mount.measurement category=system",
            "systems.*.iostat.* .host.measurement.dev.measurement category=system",
            "systems.*.loadavg.* .host.measurement.measurement category=system",
            "systems.*.memory.* .host.measurement.measurement category=system",
            "systems.*.network.* .host.measurement.dev.measurement category=system",
            "systems.*.ntpd.* .host.measurement.measurement category=system",
            "systems.*.proc.* .host.measurement.measurement category=system",
            "systems.*.sockets.* .host.measurement.measurement category=system",
            "systems.*.tcp.* .host.measurement.measurement category=system",
            "systems.*.udp.* .host.measurement.measurement category=system",
            "systems.*.users.* .host.measurement.measurement category=system",
            "systems.*.vmstat.* .host.measurement.measurement category=system",
            "systems.*.redis.* .host..env.measurement* category=redis",
            "systems.*.elasticsearch.* .host..env.measurement* category=elasticsearch",
            "systems.*.haproxy.enquos.frontend.* .host..env.measurement* category=haproxy",
            "systems.*.haproxy.* .host..env.backend.measurement* category=haproxy",
            "systems.*.nfsd.* .host...measurement* category=nfsd",
            "systems.*.nginx.* .host..measurement* category=nginx",
            "enquos.cassandra.*.jvm.* ..host.measurement* category=cassandra",
            "enquos.cassandra.*.org.apache.cassandra.metrics.ColumnFamily.* ..host...measurement...keyspace.table.measurement* category=cassandra",
            "enquos.cassandra.*.org.apache.cassandra.metrics.keyspace.* ..host...measurement...keyspace.measurement* table=_ALL_,category=cassandra",
            "enquos.cassandra.*.org.apache.cassandra.metrics.* ..host...measurement..measurement* category=cassandra",
            "enquos.scales.*.*.cassandra.request_timer.* ..env.host.measurement.measurement.agg category=app",
            "enquos.scales.*.*.cassandra.* ..env.host.measurement* category=app",
            "enquos.scales.*.*.* ..env.host.measurement* category=app",
            "enquos.timers.*.*.health_check.* ..env.host.measurement.agg category=app",
            "enquos.timers.*.*.request_duration.* ..env.host.measurement.agg category=app",
            "enquos.timers.*.*.* ..env.host.measurement..agg category=app",
            "enquos.gauges.* ..env.host.measurement* category=app",
            "enquos.counters.*.*.composite.* ..env.host..measurement.discr.agg category=app",
            "enquos.counters.*.*.validic.* ..env.host.measurement.measurement.source.kind.agg category=app",
            "enquos.counters.*.*.* ..env.host.measurement.agg category=app",
            "enquos.counters.* ..env.host.measurement.agg category=app",
            "enquos.gauges.statsd.* ..measurement* category=statsd",
            "enquos.counters.statsd.* ..measurement* category=statsd",
            "enquos.statsd.graphiteStats.* .measurement..measurement* category=graphite",
            "enquos.statsd.* .measurement* category=statsd",
        }

        test_cases := []struct {
            metric string
            expected string
        }{
            // Gauges
            {
                "enquos.gauges.statsd.timestamp_lag 11 1435077219",
                "statsd_timestamp_lag,category=statsd value=11 1435077219000000000",
            },
            // Counters
            {
                "enquos.counters.web.a1.composite.request_success.failure.count 11 1435077219",
                "request_success,agg=count,category=app,discr=failure,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a4.composite.request_route.food.count 11 1435077219",
                "request_route,agg=count,category=app,discr=food,env=web,host=a4 value=11 1435077219000000000",
            },
            {
                "enquos.counters.celery.w1.composite.chargify_subscription_cache.miss.count 11 1435077219",
                "chargify_subscription_cache,agg=count,category=app,discr=miss,env=celery,host=w1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.mobile.a1.composite.groupfinder.hit.rate 11 1435077219",
                "groupfinder,agg=rate,category=app,discr=hit,env=mobile,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a2.composite.cron_task_fired.check_all_tokens.rate 11 1435077219",
                "cron_task_fired,agg=rate,category=app,discr=check_all_tokens,env=web,host=a2 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.request_status.resp200.count 11 1435077219",
                "request_status,agg=count,category=app,discr=resp200,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.registration.init.count 11 1435077219",
                "registration,agg=count,category=app,discr=init,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.password.strength_failure.rate 11 1435077219",
                "password,agg=rate,category=app,discr=strength_failure,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.nutrition_favorite.added.count 11 1435077219",
                "nutrition_favorite,agg=count,category=app,discr=added,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.login.success.count 11 1435077219",
                "login,agg=count,category=app,discr=success,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.password_reset.init.count 11 1435077219",
                "password_reset,agg=count,category=app,discr=init,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.mobile.a1.composite.nutrition_delta.miss.count 11 1435077219",
                "nutrition_delta,agg=count,category=app,discr=miss,env=mobile,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.about_dev.attempt.count 11 1435077219",
                "about_dev,agg=count,category=app,discr=attempt,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.composite.chargify_webhook.statement_settled.count 11 1435077219",
                "chargify_webhook,agg=count,category=app,discr=statement_settled,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.web.a1.ratelimit_redis_error.count 11 1435077219",
                "ratelimit_redis_error,agg=count,category=app,env=web,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.counters.celery.w1.validic.imported.fitbit.routine.count  11 1435077219",
                "validic_imported,agg=count,category=app,env=celery,host=w1,kind=routine,source=fitbit value=11 1435077219000000000",
            },
            // Timers
            {
                "enquos.timers.web.a3.request_duration.mean_99 11 1435077219",
                "request_duration,agg=mean_99,category=app,env=web,host=a3 value=11 1435077219000000000",
            },
            {
                "enquos.timers.mobile.a1.health_check.upper 11 1435077219",
                "health_check,agg=upper,category=app,env=mobile,host=a1 value=11 1435077219000000000",
            },
            {
                "enquos.timers.web.a2.groupfinder.total.mean 11 1435077219",
                "groupfinder,agg=mean,category=app,env=web,host=a2 value=11 1435077219000000000",
            },
            // Cassandra driver
            {
                "enquos.scales.web.a3.cassandra.request_timer.99percentile 11 1435077219",
                "cassandra_request_timer,agg=99percentile,category=app,env=web,host=a3 value=11 1435077219000000000",
            },
            {
                "enquos.scales.celery.w2.cassandra.read_timeouts 11 1435077219",
                "cassandra_read_timeouts,category=app,env=celery,host=w2 value=11 1435077219000000000",
            },
        }

        p, err := graphite.NewParserWithOptions(graphite.Options{
                Templates: templates,
                Separator: "_",
        })

        if err != nil {
                t.Fatalf("unexpected error creating parser, got %v", err)
        }

        for _, tc := range test_cases {
            pt, err := p.Parse(tc.metric)
            if err != nil {
                    t.Fatalf("parse error: %v", err)
            }
            if pt.String() != tc.expected {
                    t.Errorf("parse mismatch: got %v, exp %v", pt.String(), tc.expected)
            }
        }

}
