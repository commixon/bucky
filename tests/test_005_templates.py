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


def test_apply_template():
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
            'test': 'wildcard measurement at end',
            'name': 'prod.us-west.server01.cpu.load',
            'template': 'env.zone.host.measurement*',
            'measurement': 'cpu.load',
            'tags': {'env': 'prod', 'zone': 'us-west', 'host': 'server01'},
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
    ]
    for case in cases:
        def _test_apply_template():
            template = bucky.templates.Template(case['template'])
            measurement, tags = template.process(case['name'])
            t.eq(measurement, case['measurement'])
            t.eq(tags, case.get('tags', {}))

        lbl = "Testing template application '%s': %s on %s" % (
            case['test'], case['template'], case['name']
        )
        _test_apply_template.description = lbl
        yield _test_apply_template
