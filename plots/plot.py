import prettyplotlib as ppl
import numpy as np
import pylab as pl
import matplotlib.pyplot as plt
import matplotlib as mpl
from prettyplotlib import brewer2mpl
from prettyplotlib.colors import set1 as cs
from matplotlib.ticker import FuncFormatter


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    return str(int(100 * y))

    # The percent symbol needs escaping in latex
    if mpl.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'


formatter = FuncFormatter(to_percent)


def read_csv(headers, sdss):
    folder = 'sdss' if sdss else 'sqlshare'
    with open('../results/' + folder + '/' + '_'.join(headers) + '.csv') as f:
        return np.recfromcsv(f)


def query_length():
    fig, ax = plt.subplots(1)

    data = read_csv(['lengths', 'counts'], True)
    c = data['counts'].astype(float)
    c /= sum(c)
    ppl.plot(ax, data['lengths'], np.cumsum(c), label="SDSS", color=cs[0], linewidth=2, ls='-.')

    data = read_csv(['lengths'], False)
    data.sort(order='length')
    c = data['count'].astype(float)
    c /= sum(c)
    ppl.plot(ax, data['length'], np.cumsum(c), label="SQLShare", color=cs[1], linewidth=2, ls='--')

    ppl.legend(ax, loc='lower right')

    plt.gca().yaxis.set_major_formatter(formatter)

    plt.xlabel('Query length in characters')
    plt.ylabel('% of queries')

    plt.ylim(0, 1)
    plt.xlim(0, 2500)

    plt.show()

    fig.savefig('plot_lengths_sdss.pdf', format='pdf', transparent=True)


def table_touch():
    fig, ax = plt.subplots(1)

    data = read_csv(['touch', 'counts'], True)
    ppl.bar(ax, range(len(data['touch'])), data['counts'], xticklabels=data['touch'], grid='y', log=True)

    plt.xlabel('Table touch')
    plt.ylabel('# of queries')

    plt.show()

    fig.savefig('plot_touch_sdss.pdf', format='pdf', transparent=True)

def table_touch_cda():
    fig, [ax1, ax2] = plt.subplots(1, 2, sharey=True)

    data = read_csv(['touch'], False)
    data.sort(order='touch')
    c = data['count'].astype(float)
    c /= sum(c)
    ppl.plot(ax1, data['touch'], np.cumsum(c), label="SQLShare", color=cs[0], linewidth=2, linestyle='-.')

    data = read_csv(['touch', 'counts'], True)
    c = data['counts'].astype(float)
    c /= sum(c)
    ppl.plot(ax2, data['touch'], np.cumsum(c), label="SDSS", color=cs[1], linewidth=2, linestyle='--')

    ppl.legend(ax1, loc='lower right')
    ppl.legend(ax2, loc='lower right')

    ax1.yaxis.set_major_formatter(formatter)
    ax2.yaxis.set_major_formatter(formatter)

    ax1.set_xlim([0, 500])
    ax1.set_xlim([0, 25])

    #ax1.set_xlabel('Table touch')
    fig.text(0.5, 0.02, "Table touch", ha='center')
    ax1.set_ylabel('% of queries')

    fig.subplots_adjust(wspace=0.1)

    plt.ylim(0, 1)

    plt.show()

    fig.savefig('plot_touch_cda.pdf', format='pdf', transparent=True)

def physical_ops():
    fig, ax = plt.subplots(1, figsize=(8,4))

    data = read_csv(['physical_op', 'count'], True)
    data.sort(order='count')
    data = data[-10:]

    c = data['count'].astype(float)
    c /= sum(c)
    c *= 100
    c = c.astype(int)
    ypos = np.arange(len(data['physical_op']))
    ppl.barh(ax, ypos, c, yticklabels=data['physical_op'], grid='x', annotate=True)

    #ax.set_ylabel('Physical operator')
    ax.set_xlabel('% of queries')

    #plt.subplots_adjust(bottom=.2, left=.3, right=.99, top=.9, hspace=.35)

    fig.tight_layout(rect=[0.03, 0, 1, 1])
    fig.text(0.02, 0.55, 'Physical operator', rotation=90, va='center')

    plt.show()

    fig.savefig('plot_physops_sdss.pdf', format='pdf', transparent=True)

if __name__ == '__main__':
    plt.rc('font', family='serif')

    #query_length()
    #table_touch()
    #table_touch_cda()
    #physical_ops()
