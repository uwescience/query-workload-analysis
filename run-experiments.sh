sudo apt-get update
sudo apt-get upgrade

sudo apt-get install sqlite3 libsqlite3-dev freetds-dev libfreetype6-dev texlive-full python-dev python-pip python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose libxml2-dev libxslt-dev
pip install -r requirements.txt
python setup.py develop

cd workloadanalysis
rm -f sqlshare-sdss.sqlite
rm -f sdssquerieswithplan.csv QueriesWithPlan.csv ViewsWithPlan.csv
wget https://s3-us-west-2.amazonaws.com/shrquerylogs/sdssquerieswithplan.csv
wget https://s3-us-west-2.amazonaws.com/shrquerylogs/QueriesWithPlan.csv
wget https://s3-us-west-2.amazonaws.com/shrquerylogs/ViewsWithPlan.csv
wget https://s3-us-west-2.amazonaws.com/shrquerylogs/sdss.sqlite

echo 'Consuming SQLShare logs'
qwla sqlshare consume QueriesWithPlan.csv 
qwla sqlshare consume ViewsWithPlan.csv -v

echo 'Extracting metrics of interests from SQLShare plans'
qwla sqlshare explain -q
qwla sqlshare explain -q --second

qwla sqlshare extract
qwla sqlshare analyze
qwla sqlshare analyze2 

echo 'Consuming sdss logs'
#qwla sdss consume sdssquerieswithplan.csv


echo 'Extracting metrics of interests from sdss plans'
#qwla sdss explain -q
#qwla sdss extract
qwla sdss analyze -d 'sqlite:///sdss.sqlite'


mkdir ../results/sqlshare/
mkdir ../results/sdss/
mkdir ../results/physops/
mkdir ../results/query_length/
mkdir ../results/num_dist_physops/

rm -f ../results/physops/sqlshare.csv ../results/physops/sdss.csv ../results/num_dist_physops/sqlshare.csv ../results/num_dist_physops/sdss.csv ../results/query_length/sdss.csv ../results/query_length/sqlshare.csv ../results/sqlshare/user_Q_D.csv ../results/sqlshare/view_depth.csv

echo 'Extracting result csv files...'
sqlite3 -header -csv sqlshare-sdss.sqlite 'select a.owner as user, max(a.depth) as max_depth from sqlshare_view_depths a, (select owner from sqlshare_logs group by owner order by count(*) desc limit 100) as b where a.owner = b.owner group by user order by max_depth desc' > ../results/sqlshare/view_depth.csv
sqlite3 -header -csv sqlshare-sdss.sqlite 'select replace("table", ",", "`") as "table", count(distinct(query_id)) as num_queries from sqlshare_tables group by "table" order by num_queries desc' > ../results/sqlshare/queries_per_table.csv
sqlite3 sqlshare-sdss.sqlite -csv -header "select phys_operator, count(*) from sqlshare_physops  where phys_operator != \"Clustered Index Scan\" group by phys_operator order by count(*) desc limit 10" > ../results/physops/sqlshare.csv
sqlite3 sdss.sqlite -csv -header "select phys_operator, count(*) from sdss_physops group by phys_operator order by count(*) desc limit 10" > ../results/physops/sdss.csv
sqlite3 sqlshare-sdss.sqlite -csv -header "select number, count(*) from (select query_id, count(distinct phys_operator) as number from sqlshare_physops where phys_operator != 'Clustered Index Scan' group by query_id) as f group by number order by number asc" > ../results/num_dist_physops/sqlshare.csv
sqlite3 sdss.sqlite -csv -header "select number, count(*) from (select query_id, count(distinct phys_operator) as number from sdss_physops group by query_id) as f group by number order by number asc" > ../results/num_dist_physops/sdss.csv
sqlite3 sdss.sqlite -csv -header "select length(query) as l, count(*) as c from everything group by length(query) order by length(query) asc" > ../results/query_length/sdss.csv
sqlite3 sqlshare-sdss.sqlite -csv -header "select length(query) as l, count(*) as c from sqlshare_logs group by length(query) order by length(query) asc" > ../results/query_length/sqlshare.csv
sqlite3 -header -csv sqlshare-sdss.sqlite "select a.owner, a.queries as q, b.datasets as d from (select owner, count(distinct query) as queries from sqlshare_logs group by owner) as a, (select owner, count(*) as datasets from sqlshare_logs where isview=1 group by owner) as b where a.owner = b.owner " > ../results/sqlshare/user_Q_D.csv

echo 'Generating Graphs...'
python ../plots/new_plot.py

echo "Generating the paper.pdf again"

cd ../2015-sqlshare-sigmod/
make

cd ../workloadanalysis/
rm -f sdssquerieswithplan.csv QueriesWithPlan.csv ViewsWithPlan.csv
