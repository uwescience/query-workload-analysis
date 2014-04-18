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


def read_csv(headers, sdss):
    folder = 'sdss' if sdss else 'sqlshare'
    with open('../results/' + folder + '/' + '_'.join(headers) + '.csv') as f:
        return np.recfromcsv(f)


def query_length():
    fig, ax = plt.subplots(1)

    data = read_csv(['lengths', 'counts'], True)
    c = data['counts'].astype(float)
    c /= sum(c)
    ppl.plot(ax, data['lengths'], np.cumsum(c), label="Length", color=cs[0], linewidth=2, linestyle='-.')

    data = read_csv(['compressed_lengths', 'counts'], True)
    c = data['counts'].astype(float)
    c /= sum(c)
    ppl.plot(ax, data['compressed_lengths'], np.cumsum(c), label="Compressed length", color=cs[1], linewidth=2, linestyle='--')

    ppl.legend(ax, loc='lower right')

    formatter = FuncFormatter(to_percent)
    plt.gca().yaxis.set_major_formatter(formatter)

    plt.xlabel('Query length in characters')
    plt.ylabel('% of queries')

    plt.ylim(0, 1)

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
    fig, ax = plt.subplots(1)

    data = read_csv(['touch'], False)
    data.sort(order='touch')
    c = data['count'].astype(float)
    c /= sum(c)
    ppl.plot(ax, data['touch'], np.cumsum(c), label="Touch sqlshare", color=cs[0], linewidth=2, linestyle='-.')

    #ax = ax.twinx()
    data = read_csv(['touch', 'counts'], True)
    c = data['counts'].astype(float)
    c /= sum(c)
    ppl.plot(ax, data['touch'], np.cumsum(c), label="Touch sdss", color=cs[1], linewidth=2, linestyle='--')

    ppl.legend(ax, loc='lower right')

    formatter = FuncFormatter(to_percent)
    plt.gca().yaxis.set_major_formatter(formatter)

    plt.xlabel('Table touch')
    plt.ylabel('% of queries')

    plt.ylim(0, 1)

    plt.show()

    fig.savefig('plot_touch_cda.pdf', format='pdf', transparent=True)

def physical_ops():
    fig, ax = plt.subplots(1)

    data = read_csv(['physical_op', 'count'], True)
    data.sort(order='count')
    data = data[-10:]

    c = data['count'].astype(float)
    c /= sum(c)
    c *= 100
    c = c.astype(int)
    ypos = np.arange(len(data['physical_op']))
    ppl.barh(ax, ypos, c, yticklabels=data['physical_op'], grid='x', annotate=True)

    plt.ylabel('Physical operator')
    plt.xlabel('% of queries')

    #plt.subplots_adjust(bottom=.2, left=.3, right=.99, top=.9, hspace=.35)

    plt.tight_layout(rect=[0, 0, 1, 0.6])

    plt.show()

    fig.savefig('plot_physops_sdss.pdf', format='pdf', transparent=True)

if __name__ == '__main__':
    plt.rc('font', family='serif')

    #query_length()
    #table_touch()
    #table_touch_cda()
    #physical_ops()
