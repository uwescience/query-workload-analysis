import prettyplotlib as ppl
import numpy as np
import pylab as pl
import matplotlib.pyplot as plt
import matplotlib as mpl
from prettyplotlib import brewer2mpl
from prettyplotlib.colors import set1 as cs
from prettyplotlib.colors import set2 as pcs
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


def query_length_cdf():
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

    ax.set_xlabel('Query length in characters')
    ax.set_ylabel('% of queries')

    ax.set_ylim(0, 1.01)
    ax.set_xlim(0, 2500)

    ax.yaxis.grid()

    plt.show()

    fig.savefig('plot_lengths.pdf', format='pdf', transparent=True)


def runtime_cdf():
    fig, [ax1, ax2] = plt.subplots(1, 2, sharey=True, figsize=(8, 4))

    data = read_csv(['actual', 'counts'], True)
    c = data['counts'].astype(float)
    c /= sum(c)
    ppl.plot(ax1, data['actual'], np.cumsum(c), label="SDSS", color=cs[0], linewidth=2, ls='-.')

    data = read_csv(['time_taken'], False)
    data.sort(order='time_taken')
    c = data['count'].astype(float)
    c /= 1000  # ms to seconds
    c /= sum(c)
    ppl.plot(ax2, data['time_taken'], np.cumsum(c), label="SQLShare", color=cs[1], linewidth=2, ls='--')

    ppl.legend(ax1, loc='lower right')
    ppl.legend(ax2, loc='lower right')

    plt.gca().yaxis.set_major_formatter(formatter)

    #ax.set_xlabel('Runtime in seconds')
    ax1.set_ylabel('% of queries')
    fig.text(0.5, 0.02, "Runtime in seconds", ha='center')

    ax1.yaxis.grid()
    ax2.yaxis.grid()

    fig.subplots_adjust(wspace=0.1)

    ax1.set_xlim(0, 6)
    ax2.set_xlim(0, 500)

    ax1.set_ylim(0, 1.01)
    ax2.set_ylim(0, 1.01)

    fig.tight_layout(rect=[0, .03, 1, 1])

    plt.show()

    fig.savefig('plot_runtimes_cdf.pdf', format='pdf', transparent=True)


def table_touch():
    fig, ax = plt.subplots(1)

    data = read_csv(['touch', 'counts'], True)
    ppl.bar(ax, range(len(data['touch'])), data['counts'], xticklabels=data['touch'], grid='y', log=True)

    plt.xlabel('Table touch')
    plt.ylabel('# of queries')

    plt.show()

    fig.savefig('plot_touch_sdss.pdf', format='pdf', transparent=True)


def table_touch_cdf():
    fig, [ax1, ax2] = plt.subplots(1, 2, sharey=True, figsize=(8, 4))

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

    ax1.set_xlim(0, 500)
    ax1.set_xlim(0, 25)

    ax1.yaxis.grid()
    ax2.yaxis.grid()

    #ax1.set_xlabel('Table touch')
    fig.text(0.5, 0.02, "Table touch", ha='center')
    ax1.set_ylabel('% of queries')

    fig.subplots_adjust(wspace=0.1)

    ax1.set_ylim(0, 1.01)
    ax2.set_ylim(0, 1.01)

    fig.tight_layout(rect=[0, .03, 1, 1])

    plt.show()

    fig.savefig('plot_touch_cdf.pdf', format='pdf', transparent=True)


def physical_ops():
    fig, ax = plt.subplots(1, figsize=(8, 4))

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


def logical_ops():
    fig, ax = plt.subplots(1, figsize=(8, 4))

    data = read_csv(['logical_op', 'count'], True)
    data.sort(order='count')
    data = data[-10:]

    c = data['count'].astype(float)
    c /= sum(c)
    c *= 100
    c = c.astype(int)
    ypos = np.arange(len(data['logical_op']))
    ppl.barh(ax, ypos, c, yticklabels=data['logical_op'], grid='x', annotate=True)

    #ax.set_ylabel('Physical operator')
    ax.set_xlabel('% of queries')

    #plt.subplots_adjust(bottom=.2, left=.3, right=.99, top=.9, hspace=.35)

    fig.tight_layout(rect=[0.03, 0, 1, 1])
    fig.text(0.02, 0.55, 'Logical operator', rotation=90, va='center')

    plt.show()

    fig.savefig('plot_logops_sdss.pdf', format='pdf', transparent=True)


def opcounts():
    fig, ax = plt.subplots(1, figsize=(8, 4))

    data = read_csv(['logops', 'counts'], True)

    #ppl.bar(ax, data['ops'], data['counts'], grid='y', log=True)
    ppl.hist(ax, data['logops'], weights=data['counts'], bins=25, grid='y', log=True, facecolor=pcs[0])

    ax.set_xlabel('Logical operators used (binned)')
    ax.set_ylabel('# of queries')

    ax.set_xlim(0, 110)

    fig.tight_layout()
    plt.show()

    fig.savefig('logops_query.pdf', format='pdf', transparent=True)


if __name__ == '__main__':
    plt.rc('font', family='serif')

    #query_length_cdf()
    #table_touch()
    #table_touch_cdf()
    #physical_ops()
    #logical_ops()
    #runtime_cdf()
    #opcounts()
