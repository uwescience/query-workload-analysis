#!/usr/bin/env python
# a bar plot with errorbars
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from collections import OrderedDict as od
import sys

def parse(fname):
  d = od()
  for line in file(fname):
    line = ''.join([i if ord(i) < 128 else ' ' for i in line])
    toks = line.split()
    num, category, operator = toks[0], toks[1], ' '.join(toks[2:])
    d[(category, operator)] = num
  return d

fnames = ("tpch", "sdss", "sqlshare")

rawdata = map(parse, ["%s_operator_diversity.txt" % n for n in fnames])

full_disjunction = od()

def filldata(i):
  for (k,v) in rawdata[i].iteritems():
    tup = full_disjunction.setdefault(k,[0,0,0])
    tup[i] = int(v)

M = len(fnames)
data = [[]]*M
# compute the full disjunction / outer join
for i in range(M):
  filldata(i)

toplot = dict([(k,v) for (k,v) in full_disjunction.items() if k[0] == "compare"])

# extract the values
for i in range(M):
  tot = sum([v[i] for v in toplot.values()])
  data[i] = [100*float(v[i])/tot for v in toplot.values()]

# now plot it

sns.set_context("paper", font_scale=1.5, rc={"lines.linewidth": 2.5})

N = len(toplot.keys())
ind = np.arange(N) + 0.125  # the x locations for the groups
width = 0.25      # the width of the bars

fig, ax = plt.subplots()

b, g, r = sns.color_palette("deep", 3)
rects3 = ax.bar(ind+width*2, data[2], width, color=r)
rects2 = ax.bar(ind+width, data[1], width, color=g)
rects1 = ax.bar(ind, data[0], width, color=b)


# add some text for labels, title and axes ticks
ax.set_ylabel('% of expression operators')
# ax.set_title('Operator usage by workload')
ax.set_xticks(ind+width*1.5)
labels = [k[1] for k in toplot.keys()]
ax.set_xticklabels( labels )
# ax.set_ylim((0,100))

ax.legend( (rects1[0], rects2[0], rects3[0]), ["TPC-H", "SDSS", "SQLShare"], loc=1)

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        #ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
        #        ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

locs, labels = plt.xticks()
plt.setp(labels, rotation=45)

plt.tight_layout()
plt.savefig('../plot_compares.pdf', format='pdf')
plt.show()
