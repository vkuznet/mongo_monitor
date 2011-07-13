#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
File: mongo_monitor.py
Author: Valentin Kuznetsov <vkuznet [at] gmail [dot] com>
Description: Monitoring MongoDB server
"""

# system modules
import os
import sys
import time
import thread
import pprint
import hashlib
import datetime
from   optparse import OptionParser

# web modules
import cherrypy
from   cherrypy import expose, HTTPError, tools, response
from   cherrypy import config as cherryconf

# mongo modules
from pymongo import Connection

class MOptionParser: 
    """Option parser"""
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("--uri", action="store", type="string", 
             default="mongodb://localhost:27017", dest="uri",
             help="specify MongoDB URI, e.g. mongodb://localhost:27017")
        self.parser.add_option("--dbcoll", action="store", type="string", 
             default="mongodb.stats", dest="dbcoll",
             help="db.collection to store MongoDB statistics, \
                        default mongodb.stats")
        self.parser.add_option("--dbsize", action="store", type="int", 
             default=10*1024*1024, dest="dbsize",
             help="size of MongoDB statistics collection, default 10MB")
        self.parser.add_option("--interval", action="store", type="int", 
             default=5, dest="interval",
             help="update interval, default 5 sec.")
    def get_opt(self):
        """Returns parse list of options"""
        return self.parser.parse_args()

def template_top(title="Test", dburi=None):
    """Return top html tempalte"""
    page  = "<!DOCTYPE HTML>\n<html><head>"
    page += "<title>%s</title>" % title
    page += """<script type="text/javascript" src="/js/protovis-r3.2.js"></script>
<style type="text/css" media="screen">
body {
    font-size:12px;font-family:Verdana,Arial,sans-serif;height:100%;
    padding:10px;background-color:#DBDBDB;margin-top:-10px;
} 
.big {font-size: 36px;font-weight:bold;}
.centerBox {
    position:absolute;left:50%;width:900px;margin-left:-450px;min-height:100%;
    vertical-align:top;background-color:#fff;padding-left:10px;padding-right:10px
}
.shadow {
    -webkit-box-shadow: 0px 0px 5px 5px #666666;
    -moz-box-shadow: 0px 0px 5px 5px #666666;
    -o-box-shadow: 0px 0px 5px 5px #666666;
    box-shadow: 0px 0px 5px 5px #666666;
}
.bottom {font-size:10px;color:#888888;margin-left:150px;}
.hide {display: none;}
.show {display: block;margin-top:6px;border-top-width:1px; }
h3 {color: #4D5B99;border-bottom: 1px dotted #555;}
hr.line {width: 100%;border: none;border-top: 1px dotted #CCCCCC;}
a:link {font-family: Verdana, Arial, sans-serif;
        color: #4D5B99;text-decoration: none;border-bottom: 1px dotted #555;}
a:visited {font-family: Verdana, Arial, sans-serif;
        color: #48468f;text-decoration: none;border-bottom: 1px dotted #555;}
a:hover {font-family: Verdana, Arial, sans-serif;
        color: white;background-color:#8c8ad0;}
a[name] {color: inherit;text-decoration: inherit;}
</style></head><body>"""
    page += """<div class="centerBox shadow">"""
    page += """<div class="big">%s</div>""" % dburi
    return page

def template_bottom():
    """Return bottom html template"""
    license = '<a href="http://www.gnu.org/licenses/gpl.html">GPL</a>'
    author  = '<a href="http://www.linkedin.com/pub/valentin-kuznetsov/15/167/ab6">V.Kuznetsov</a>'
    pkgs    = '<a href="http://www.python.org/">Python</a>, '
    pkgs   += '<a href="http://www.cherrypy.org/">CherryPy</a>, '
    pkgs   += '<a href="http://vis.stanford.edu/protovis/">Protovis</a>'
    page    = '<br/><br/><br/><hr class="line"/>'
    page   += '<div class="bottom">Author: %s, License: %s, build with: %s</div>' \
                % (author, license, pkgs)
    page   += '</div></body></html>'
    return page

def template_server_info(sinfo):
    """
    Tempalte for server info
    """
    page  = '\n<h3>SERVER INFO</h3>\n<ul>'
    for key, val in sinfo.items():
        page += '\n<li><b>%s</b>: %s</li>' % (key, val)
    page += '\n</ul>'
    return page

def genkey(value):
    """
    Generate a new key-hash for a given value. We use md5 hash for the
    query and key is just hex representation of this hash.
    """
    keyhash = hashlib.md5()
    keyhash.update(value)
    return keyhash.hexdigest()

def collection_info(coldict):
    """
    Template for collection info
    """
    page  = ""
    for name, colinfo in coldict.items():
        page += '<b>Collection:</b> %s, ' % name
        page += '<pre>%s</pre>' % pprint.pformat(colinfo).replace("u'", "'")
        page += '<br/>'
    return page

def template_db_info(dbinfo):
    """
    Template for db info
    """
    page  = '\n<h3>DATABASE INFO</h3>\n'
    page += """<script type="text/javascript">
function FlipTag(tag) {var id=document.getElementById(tag);
if (id) {if  (id.className == "show") {id.className="hide";} else {id.className="show"; }}}
</script>"""
    page += '<ul>'
    for key, colinfo in dbinfo.items():
        kid   = 'id_%s' % genkey(key)
        page += '\n<li>'
        page += """\n<b><a href="javascript:FlipTag('%s')">%s</a></b>""" \
                % (kid, key)
        page += '<div id="%s" class="hide">%s</div>' \
                % (kid, collection_info(colinfo))
        page += '</li>'
    page += '</ul>'
    return page

def template_plot_form(attributes, default=None):
    """
    Tempalte to build HTML form to plot parameters
    """
    page  = '\n<h3>MongoDB parameters</h3>'
    page += '\n<form action="/stat">\n<select name="attr">'
    for attr in attributes:
        if  default and default == attr:
            page += '\n<option selected="selected">%s</option>' % attr
        else:
            page += '\n<option>%s</option>' % attr
    page += '\n</select>'
    page += '\ntime1: <input type="text" name="t1" value="%s" />' \
    % str(datetime.datetime.utcfromtimestamp(time.time()-3600)).split('.')[0]
    page += '\ntime2: <input type="text" name="t2" value="%s" />' \
    % str(datetime.datetime.utcfromtimestamp(time.time())).split('.')[0]
    page += '\n<input type="submit" value="Plot"/>'
    page += '\n</form>'
    return page

def template_plot(spec):
    """
    Return protovis template. Provided spec is a dict of the following keys:
    data  - array of JSON data to be plotted
    ymin  - min value for Y axis
    ymax  - max value for Y axis
    xmin  - min value for X axis
    xmax  - max value for X axis
    title - title for the plot
    attr  - a key attribute to be plotted from array of JSON data
    """
    page = """<script type="text/javascript+protovis">
var data = %(data)s;
var width = 560;
var height = 245;
var ymax = %(ymax)s;
var ymin = %(ymin)s;
var yratio = (ymax-ymin)/height;
var xmax = "%(xmax)s";
var xmin = "%(xmin)s";
var barWidth = width/data.length;
var x = pv.Scale.linear(xmin, xmax).range(0, width);
var y = pv.Scale.linear(ymin, ymax).range(0, height);
var barWidth = width/data.length;
var plot = new pv.Panel().width(width).height(height)
    .bottom(30).left(100).right(10).top(5);
/* X-axis ticks. */
plot.add(pv.Rule).data(x.ticks()).left(x).strokeStyle(function(d) d ? "#bbb" : "#000")
  .anchor("bottom").add(pv.Label).text(x.tickFormat);
plot.add(pv.Rule).bottom(0).width(width);

/* Y-axis ticks. */
plot.add(pv.Rule).data(y.ticks(5)).bottom(y).strokeStyle(function(d) d ? "#bbb" : "#000")
  .anchor("left").add(pv.Label).text(y.tickFormat);

plot.add(pv.Line).data(data).interpolate("step-after").lineWidth(2)
        .bottom(function(d) (d.%(attr)s-ymin)/yratio)
        .left(function() this.index * barWidth);
        //.bottom(function(d) d.%(attr)s * (height/ymax))

plot.add(pv.Label).font("bold 14px sans-serif")
    .left(width/2).bottom(-30).textAlign("center")
    .text("time in %(units)s for [%(time1)s, %(time2)s]");

plot.add(pv.Label).font("bold 14px sans-serif")
    .left(-50).bottom(height/4).textAngle(-1.571).text("%(title)s");
plot.render();
</script>
""" % spec
    return page

def exposejs (func):
    """CherryPy expose JavaScript decorator"""
    @expose
    def wrapper (self, *args, **kwds):
        """Decorator wrapper"""
        data = func (self, *args, **kwds)
        cherrypy.response.headers['Content-Type'] = "text/javascript"
        return data
    return wrapper

def minify(content):
    """
    Remove whitespace in provided content.
    """
    content = content.replace('\n', ' ')
    content = content.replace('\t', ' ')
    content = content.replace('   ', ' ')
    content = content.replace('  ', ' ')
    return content

def set_headers(itype, size=0):
    """
    Set response header Content-type (itype) and Content-Length (size).
    """
    if  size > 0:
        response.headers['Content-Length'] = size
    response.headers['Content-Type'] = itype
    response.headers['Expires'] = 'Sat, 14 Oct 2017 00:59:30 GMT'

def db_updater(dburi, dbname, dbcoll, interval):
    """Update MognoDB with its statistics"""
    conn = Connection(dburi)
    coll = conn[dbname][dbcoll]
    while True:
        data = conn[dbname].command( { "serverStatus" : 1 } )
        coll.insert(data)
        time.sleep(interval)

def convert_timestamp(timestamp):
    """
    Convert given timestamp to format suitable for plotting
    """
    return str(timestamp).split('.')[0]

def delta(time1, time2):
    """
    Calculate delta for provided time range. Return
    units, min, max values.
    """
    sec1 = time.mktime(time1.timetuple())
    sec2 = time.mktime(time2.timetuple())
    diff = sec2 - sec1
    day  = 24*60*60
    hour = 60*60
    if  diff > day:
        return 'days', 0, float(diff)/day
    elif diff > hour and diff < day:
        return 'hours', 0, float(diff)/hour
    else:
        return 'seconds', 0, diff

def parse_timestamp(timestamp):
    """
    Parse given timestamp to datetime object.
    """
    try:
        return datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    except:
        raise HTTPError(500, \
            'Invalid time parameter format, must be in YYYY-MM-DD HH:MM:SS')

class MongoMonitor(object):
    """
    MongoDB Monitor class. It sets daemon to store into MongoDB its
    own stat information and provide web functionality.
    """
    def __init__(self, dburi, dbname='mongodb', dbcoll='stats', 
                        size=10*1024*1024, interval=5):
        self.dburi = dburi
        self.conn  = Connection(dburi)
        if  dbname not in self.conn.database_names():
            dbptr  = self.conn[dbname]
            dbptr.create_collection(dbcoll, {'capped':True, 'size':size})
        self.coll  = self.conn[dbname][dbcoll]
        self.attr  = []
        for key, val in self.conn[dbname].command( { "serverStatus" : 1 } ).items():
            if  isinstance(val, dict):
                for akey, aval in val.items():
                    if  isinstance(aval, dict):
                        for kkk, vvv in aval.items():
                            if  not isinstance(vvv, dict):
                                self.attr.append('%s.%s.%s' % (key, akey, kkk))
                    else:
                        self.attr.append('%s.%s' % (key, akey))

        # start thread with db_updater
        thread.start_new_thread(db_updater, (dburi, dbname, dbcoll, interval))

        # setup js/css dirs
        self.jsdir  = '%s/%s' % (__file__.rsplit('/', 1)[0], 'js')
        if  os.environ.has_key('JSPATH'):
            self.jsdir = os.environ['JSPATH']
        if  not os.path.isdir(self.jsdir):
            raise Exception('JS path is not set')
        # To be filled at run time
        self.cssmap = {}
        self.jsmap  = {}

        # Update CherryPy configuration
        mime_types  = ['text/css']
        mime_types += ['application/javascript', 'text/javascript',
                       'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.encode.on': True, 
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                           'request.show_tracebacks': False,
                           'server.environment':'production',
                           'server.socket_host':'0.0.0.0',
                          })
        self._cache = {}

    def server_info(self):
        """Get server info"""
        return self.conn.server_info()

    def db_info(self):
        """
        Get DB info
        """
        ddict = {}
        for database in self.conn.database_names():
            collections = self.conn[database].collection_names()
            coll = self.conn[database]
            info_dict = {}
            for cname in collections:
                info_dict[cname] = coll.command({'collStats': cname})
            ddict[database] = info_dict
        return ddict

    def get_stats(self, attr, datetime1, datetime2):
        """
        Get MongoDB statistics for provided time interval
        """
        spec = {'localTime' : {'$gte': datetime1, '$lte': datetime2}}
        alist = attr.split('.')
        if  len(alist) == 2:
            key, att = alist
            alast = None
        elif len(alist) == 3:
            key, att, alast = alist
        else:
            raise Exception('Too many attributes')
        for row in self.coll.find(spec):
            if  row.has_key(key) and row[key].has_key(att):
                value = row[key][att]
                if  alast:
                    value = value[alast]
                if  isinstance(value, datetime.datetime):
                    value = time.mktime(datetime.datetime.timetuple(value))
                if  alast:
                    rec = {key : {att: {alast: value}}}
                else:
                    rec = {key : {att: value}}
                yield rec

    def minmaxval(self, attr, datetime1, datetime2):
        """
        Get MongoDB statistics for provided time interval
        """
        spec   = {'localTime' : {'$gte': datetime1, '$lte': datetime2}}
        maxval = 0
        minval = 0
        alist  = attr.split('.')
        if  len(alist) == 2:
            key, att = alist
            alast = None
        elif len(alist) == 3:
            key, att, alast = alist
        else:
            raise Exception('Too many attributes')
        for row in self.coll.find(spec):
            value = row[key][att]
            if  alast:
                value = value[alast]
            if  isinstance(value, datetime.datetime):
                value = time.mktime(datetime.datetime.timetuple(value))
            if  not minval:
                minval = value
            if  value > maxval:
                maxval = value
            if  value < minval:
                minval = value
        return minval, maxval

    @expose
    def index(self):
        """Main page"""
        page  = template_server_info(self.server_info())
        page += template_db_info(self.db_info())
        page += template_plot_form(self.attr)
        return self.page('MongoDB', page)

    @expose
    def page(self, title, content):
        """Format HTML page"""
        return template_top(title, self.dburi) + content + template_bottom()

    @expose
    def stat(self, **kwds):
        """Plot given set of parameters"""
        time1 = kwds.get('t1')
        time2 = kwds.get('t2')
        if  not time1 and not time2: # default is 1 hour
            time1 = datetime.datetime.utcfromtimestamp(time.time()-3600)
            time2 = datetime.datetime.utcfromtimestamp(time.time())
        else:
            time1 = parse_timestamp(time1)
            time2 = parse_timestamp(time2)
        attr  = kwds.get('attr')
        if  attr not in self.attr:
            raise HTTPError(500, 'Unknown attribute %s, supported: %s' \
                        % (attr, self.attr))
        data  = [r for r in self.get_stats(attr, time1, time2)]
        minval, maxval = self.minmaxval(attr, time1, time2)
        units, xmin, xmax = delta(time1, time2)
        spec  = {'data':str(data).replace("u'", "'"), 'attr':attr,
                 'title':attr, 'ymax':maxval, 'ymin':minval,
                 'units': units, 'xmin': xmin, 'xmax': xmax,
                 'time1': convert_timestamp(time1),
                 'time2': convert_timestamp(time2)}
        content = template_plot(spec)
        page    = template_server_info(self.server_info())
        page   += template_db_info(self.db_info())
        page   += template_plot_form(self.attr, attr)
        page   += content
        return self.page('MongoDB', page)

    @exposejs
    @tools.gzip()
    def js(self, *args, **kwargs):
        """
        Serve protovis JS file.
        """
        args = ['/'.join(args)] # preserve YUI dir structure
        scripts = self.check_scripts(args, self.jsmap, self.jsdir)
        return self.serve_files(args, scripts, self.jsmap)

    def check_scripts(self, scripts, resource, path):
        """
        Check a script is known to the resource map 
        and that the script actually exists   
        """           
        for script in scripts:
            if  script not in resource.keys():
                spath = os.path.normpath(os.path.join(path, script))
                if  os.path.isfile(spath):
                    resource.update({script: spath})
        return scripts

    def serve_files(self, args, scripts, resource, datatype='', minimize=False):
        """
        Return asked set of files for JS, CSS.
        """
        idx = "-".join(scripts)
        if  idx not in self._cache.keys():
            data = ''
            if  datatype == 'css':
                data = '@CHARSET "UTF-8";'
            for script in args:
                path = os.path.join(sys.path[0], resource[script])
                path = os.path.normpath(path)
                ifile = open(path)
                data = "\n".join ([data, ifile.read().\
                    replace('@CHARSET "UTF-8";', '')])
                ifile.close()
            if  datatype == 'css':
                set_headers("text/css")
            if  minimize:
                self._cache[idx] = minify(data)
            else:
                self._cache[idx] = data
        return self._cache[idx] 


if  __name__ == '__main__':
    omgr = MOptionParser()
    (opts, args) = omgr.get_opt()
    dbname, dbcoll = opts.dbcoll.split('.')
    mmgr = MongoMonitor(opts.uri, dbname, dbcoll, opts.dbsize, opts.interval)
    cherrypy.quickstart(mmgr, '/')
