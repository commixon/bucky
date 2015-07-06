import bucky.templates


import t


def test_filter_tree():
    """Test parser's filter tree"""
    parser = bucky.templates.NameParser([
        '*.* .wrong.measurement*',
        'servers.* .host.measurement*',
        'servers.localhost .host.measurement*',
        '*.localhost .host.measurement*',
        '*.*.cpu .host.measurement*',
        'a.b.c .host.measurement*',
        'influxd.*.foo .host.measurement*',
        'prod.*.mem .host.measurement*',
    ])

    # recursively check sanity of filter tree data structure
    parser.filter_tree.check_tree()

    tree = ('ROOT', (
        ('a', (
            ('b', (
                ('c', '.host.measurement*'),
            )),
        )),
        ('influxd', (
            ('*', (
                ('foo', '.host.measurement*'),
            )),
        )),
        ('prod', (
            ('*', (
                ('mem', '.host.measurement*'),
            )),
        )),
        ('servers', (
            ('localhost', '.host.measurement*'),
            ('*', '.host.measurement*')
        )),
        ('*', (
            ('localhost', '.host.measurement*'),
            ('*', (
                ('cpu', '.host.measurement*'),
            ))
        ))
    ))

    t.eq(parser.filter_tree.get_tuple_tree(), tree)


def test_template():
    cases = [
        {
            'test': 'metric only',
            'name': 'cpu',
            'template': 'measurement',
            'measurement': 'cpu',
        },
        {
            'test': 'metric with single series',
            'name': 'cpu.server01',
            'template': 'measurement.hostname',
            'measurement': 'cpu',
            'tags': {'hostname': 'server01'},
        },
        {
            'test': 'metric with multiple series',
            'name': 'cpu.us-west.server01',
            'template': 'measurement.region.hostname',
            'measurement': 'cpu',
            'tags': {'hostname': 'server01', 'region': 'us-west'},
        },
        {
            'test': 'ignore unnamed',
            'name': 'foo.cpu',
            'template': 'measurement',
            'measurement': 'foo',
        },
        {
            'test': 'name shorter than template',
            'name': 'foo',
            'template': 'measurement.A.B.C',
            'measurement': 'foo',
        },
        {
            'test': 'skip fields',
            'name': 'ignore.us-west.ignore-this-too.cpu.load',
            'template': '.zone..measurement*',
            'measurement': 'cpu.load',
            'tags': {'zone': 'us-west'},
        },
        {
            'test': 'no measurement specified in template',
            'name': 'localhost.cpu',
            'template': 'host.metric',
            'measurement': 'localhost.cpu',
            'tags': {'host': 'localhost', 'metric': 'cpu'},
        },
        {
            'test': 'wildcard measurement at end',
            'name': 'prod.us-west.server01.cpu.load',
            'template': 'env.zone.host.measurement*',
            'measurement': 'cpu.load',
            'tags': {'env': 'prod', 'zone': 'us-west', 'host': 'server01'},
        },
        {
            'test': 'two measurement parts',
            'name': 'prod.us-west.server01.cpu.load',
            'template': 'env.zone.measurement.plugin.measurement',
            'measurement': 'server01.load',
            'tags': {'env': 'prod', 'zone': 'us-west', 'plugin': 'cpu'}
        },
        {
            'test': 'three measurement parts, last with wildcard',
            'name': 'prod.us-west.server01.cpu.load.shortterm',
            'template': 'measurement.zone.measurement.plugin.measurement*',
            'measurement': 'prod.server01.load.shortterm',
            'tags': {'zone': 'us-west', 'plugin': 'cpu'}
        },
    ]
    for case in cases:
        def _test_template():
            template = bucky.templates.Template(case['template'])
            measurement, tags = template.process(case['name'])
            t.eq(measurement, case['measurement'])
            t.eq(tags, case.get('tags', {}))

        _test_template.description = "Template test '%s': %s on %s" % (
            case['test'], case['template'], case['name']
        )
        yield _test_template


def test_parser():
    cases = [
        {
            'test': 'match default',
            'name': 'cpu',
            'measurement': 'cpu',
        },
        {
            'test': 'match multiple measurement',
            'templates': 'servers.localhost.* .host.measurement.measurement*',
            'name': 'servers.localhost.cpu.cpu_load.10',
            'measurement': 'cpu.cpu_load.10',
            'tags': {'host': 'localhost'},
        },
        {
            'test': 'match multiple measurement with separator',
            'separator': '_',
            'templates': 'servers.localhost.* .host.measurement.measurement*',
            'name': 'servers.localhost.cpu.cpu_load.10',
            'measurement': 'cpu_cpu_load_10',
            'tags': {'host': 'localhost'},
        },
        {
            'test': 'match wildcard',
            'templates': 'servers.* .host.measurement*',
            'name': 'servers.localhost.cpu_load',
            'measurement': 'cpu_load',
            'tags': {'host': 'localhost'},
        },
        {
            'test': 'match exact before wildcard',
            'templates': [
                'servers.* .wrong.measurement*',
                'servers.localhost .host.measurement*',
            ],
            'name': 'servers.localhost.cpu_load',
            'measurement': 'cpu_load',
            'tags': {'host': 'localhost'},
        },
        {
            'test': 'longest match first',
            'templates': [
                '*.* .wrong.measurement*',
                'servers.* .wrong.measurement*',
                'servers.localhost .wrong.measurement*',
                'servers.localhost.cpu .host.resource.measurement*',
                '*.localhost .wrong.measurement*'
            ],
            'name': 'servers.localhost.cpu.cpu_load',
            'measurement': 'cpu_load',
            'tags': {'host': 'localhost', 'resource': 'cpu'},
        },
        {
            'test': 'match multiple wildcards',
            'templates': [
                '*.* .wrong.measurement*',
                'servers.* .host.measurement*',
                'servers.localhost .wrong.measurement*',
                '*.localhost .wrong.measurement*',
            ],
            'name': 'servers.server01.cpu_load',
            'measurement': 'cpu_load',
            'tags': {'host': 'server01'},
        },
    ]
    for case in cases:
        def _test_parser():
            parser = bucky.templates.NameParser(case.get('templates', ''),
                                                case.get('separator', '.'),
                                                case.get('global_tags'))
            measurement, tags = parser.process(case['name'])
            t.eq(measurement, case['measurement'])
            t.eq(tags, case.get('tags', {}))

        _test_parser.description = "Parser test '%s'." % case['test']
        yield _test_parser
