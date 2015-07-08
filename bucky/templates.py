import bisect
import logging


log = logging.getLogger(__name__)


class ProtocolError(Exception):
    pass


class Node(object):
    """Node of a sorted filter tree"""

    def __init__(self, value, template):
        """Initalize a filter node in a sorted tree, creating grandchildren"""
        parts = value.split('.', 1)
        self.value = parts[0]
        subvalue = parts[1] if len(parts) > 1 else ''
        self.template = template if not subvalue else ''
        self.children = []
        if subvalue:
            self.add_child(subvalue, template)

    def add_child(self, value, template):
        """Add and return (grand)child for given value to children tree"""
        parts = value.split('.', 1)
        subvalue = parts[1] if len(parts) > 1 else ''
        child = self.find_child(parts[0])
        if child:
            if subvalue:
                return child.add_child(subvalue, template)
            elif not child.template:
                child.template = template
        else:
            child = Node(value, template)
            # Add child to children while preserving order
            bisect.insort(self.children, child)
        return child

    def find_child(self, value):
        """Search for child for value using binary search"""
        index = bisect.bisect_left(self.children, Node(value, ''))
        if index != len(self.children):
            child = self.children[index]
            if child.value == value:
                return child

    def find_match(self, value):
        """Find if a leaf node further down the tree matches this value"""
        parts = value.split('.', 1)
        value = parts[0]
        subvalue = parts[1] if len(parts) > 1 else ''
        child = self.find_child(value)
        if child:
            if subvalue:
                return child.find_match(subvalue)
            return child
        if self.children and self.children[-1].value == '*':
            child = self.children[-1]
            if subvalue:
                return child.find_match(subvalue)
            return child
        if self.template:
            return self

    def __cmp__(self, node):
        """Compare magic method used to sort children nodes"""
        if self.value == '*' and node.value != '*':
            return 1
        if self.value != '*' and node.value == '*':
            return -1
        return cmp(self.value, node.value)

    def __str__(self):
        if self.children:
            return '%s: %d children' % (self.value, len(self.children))
        else:
            return '%s: %s, %s' % (self.value,
                                   self.template.template, self.template.tags)

    def __repr__(self):
        return 'Node %s' % str(self)

    def rec_str(self, offset=0, prefix=' '):
        msg = prefix * offset + str(self)
        for child in self.children:
            msg += '\n' + child.rec_str(offset + 1, prefix)
        return msg

    def check_tree(self):
        """Perform sorted tree data structure sanity checks"""
        assert self.value
        assert self.template or self.children
        values = [child.value for child in self.children]
        assert self.children == sorted(self.children), values
        assert len(set(values)) == len(values), values
        assert len(filter(bool, values)) == len(values), values
        for child in self.children:
            child.check_tree()

    def get_tuple_tree(self):
        if self.children:
            return self.value, tuple(child.get_tuple_tree()
                                     for child in self.children)
        return self.value, self.template.template


class Template(object):
    """Template object like the ones used by Influxdb graphite write plugin"""

    def __init__(self, line, separator='.', global_tags=None):
        """Parse template line and initialize template instance"""
        parts = filter(bool, line.strip().split())
        if not 1 <= len(parts) <= 3:
            raise ProtocolError("Invalid template line: %s" % line)
        self.match = '*'
        self.separator = separator
        self.tags = global_tags.copy() if global_tags else {}
        if len(parts) == 1:
            self.template = parts[0]
        else:
            self.match = parts[0]
            self.template = parts[1]
            if len(parts) == 3:
                for tagpart in parts[2].split(','):
                    tagkv = tagpart.split('=')
                    if not len(tagkv) == 2:
                        raise ProtocolError("Invalid tag part: %s" % tagpart)
                    self.tags[tagkv[0]] = tagkv[1]

    def process(self, name):
        """Apply template to metric name and extract measurement and tags"""
        fields = name.split('.')
        measurement_parts = []
        tags = self.tags.copy()
        for i, tag in enumerate(self.template.split('.')):
            if i >= len(fields):
                break
            if tag == 'measurement':
                measurement_parts.append(fields[i])
            elif tag == 'measurement*':
                measurement_parts.extend(fields[i:])
            elif tag:
                tags[tag] = fields[i]
        if not measurement_parts:
            measurement_parts = fields
        return self.separator.join(measurement_parts), tags

    def __str__(self):
        return '%s -> %s' % (self.match, self.template)

    def __repr__(self):
        return 'Template %s' % str(self)


class NameParser(object):
    """Helper class to construct InfluxDB measurement and tags from name"""

    def __init__(self, lines='', separator='.', tags=None):
        """Initialize InfluxDBNameParser

        separator: use this when combining multiple metric name parts
            in InfluxDB measurement name.
        tags: global default tags to apply to all processed metrics

        """
        if isinstance(lines, basestring):
            lines = lines.splitlines()
        lines = [line.strip() for line in lines if line]
        self.filter_tree = Node('ROOT', '')
        for line in lines:
            log.debug("InfluxDBNameParser template line '%s'.", line)
            template = Template(line, separator, tags)
            self.filter_tree.add_child(template.match, template)
        self.filter_tree.add_child('*', Template('* measurement*'))
        self.filter_tree.check_tree()

    def process(self, name):
        """Process a name and return measurement and tags"""
        node = self.filter_tree.find_match(name)
        if node:
            return node.template.process(name)
        log.warning("No template found for '%s', not even default template.")
        return name, {}

    def __call__(self, name):
        """Call process on name, allows instance to be used as a function"""
        return self.process(name)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print "Supply config file path."
        sys.exit(1)
    logfmt = "[%(asctime)-15s][%(levelname)s] %(module)s - %(message)s"
    loglvl = logging.DEBUG
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logfmt))
    handler.setLevel(loglvl)
    logging.root.addHandler(handler)
    logging.root.setLevel(loglvl)
    with open(sys.argv[1]) as fobj:
        lines = fobj.readlines()
    parser = NameParser(lines)
