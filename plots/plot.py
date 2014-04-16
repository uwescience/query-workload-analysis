import prettyplotlib as ppl
import numpy as np
import pylab as pl
import matplotlib.pyplot as plt
import matplotlib as mpl
from prettyplotlib import brewer2mpl
from prettyplotlib.colors import set1 as cs


def read_csv(headers):
    with open('../results/' + '_'.join(headers) + '.csv') as f:
        return np.recfromcsv(f)


def query_length():
    fig, ax = plt.subplots(1)

    data = read_csv(['lengths', 'counts'])
    ppl.plot(ax, data['lengths'], np.cumsum(data['counts']), label="Length", color=cs[0], linewidth=2, linestyle='-.')

    data = read_csv(['compressed_lengths', 'counts'])
    ppl.plot(ax, data['compressed_lengths'], np.cumsum(data['counts']), label="Compressed length", color=cs[1], linewidth=2, linestyle='--')

    ppl.legend(ax, loc='lower right')

    plt.xlabel('Query length in characters')
    plt.ylabel('# of queries')

    plt.show()

    fig.savefig('plot_lengths_sdss.pdf', format='pdf', transparent=True)


def table_touch():
    fig, ax = plt.subplots(1)

    data = read_csv(['touch', 'counts'])
    ppl.bar(ax, range(len(data['touch'])), data['counts'], xticklabels=data['touch'], grid='y', log=True)

    plt.xlabel('Table touch')
    plt.ylabel('# of queries')

    plt.show()

    fig.savefig('plot_touch_sdss.pdf', format='pdf', transparent=True)


def logical_ops():
    fig, ax = plt.subplots(1)

    data = read_csv(['logical_op', 'count'])
    ppl.barh(range(len(data['logical_op'])), data['count'])

    plt.xlabel('Logical operators')
    plt.ylabel('# of queries')

    plt.show()

    fig.savefig('plot_logops_sdss.pdf', format='pdf', transparent=True)

if __name__ == '__main__':
    plt.rc('font', family='serif')

    #query_length()
    #table_touch()
    logical_ops()
