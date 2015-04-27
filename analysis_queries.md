# Queries to get the numbers for the paper

Get csv
```
\f ','
\a
```

### Runtimes

```sql
select runtime, count(*) from sqlshare_logs group by runtime order by runtime asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select elapsed as runtime, count(*) from logs group by elapsed order by elapsed asc" > results/runtime/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select 1.0*runtime/1000 as runtime, count(*) from sqlshare_logs group by runtime order by runtime asc" > results/runtime/sqlshare.csv
```

### Query length

```sql
select char_length(query), count(*) from tpchqueries group by char_length(query) order by char_length(query) asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select char_length(query), count(*) from tpchqueries group by char_length(query) order by char_length(query) asc" > results/query_length/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select char_length(query), count(*) from logs group by char_length(query) order by char_length(query) asc" > results/query_length/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select char_length(query), count(*) from sqlshare_logs group by char_length(query) order by char_length(query) asc" > results/query_length/sqlshare.csv
```

### Histogram for logops counts

```sql
select number, count(*) from (select query_id, count(*) as number from tpch_logops group by query_id) as f group by number order by number asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(*) as number from tpch_logops group by query_id) as f group by number order by number asc" > results/num_logops/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(*) as number from sdss_logops group by query_id) as f group by number order by number asc" > results/num_logops/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(*) as number from sqlshare_logops group by query_id) as f group by number order by number asc" > results/num_logops/sqlshare.csv
```


### Histogram for phys counts

```sql
select number, count(*) from (select query_id, count(*) as number from tpch_physops group by query_id) as f group by number order by number asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(*) as number from tpch_physops group by query_id) as f group by number order by number asc" > results/num_physops/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(*) as number from sdss_physops group by query_id) as f group by number order by number asc" > results/num_physops/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(*) as number from sqlshare_physops group by query_id) as f group by number order by number asc" > results/num_physops/sqlshare.csv
```


### Distinct physops

```sql
select number, count(*) from (select query_id, count(distinct phys_operator) as number from tpch_physops group by query_id) as foo group by number order by number asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct phys_operator) as number from tpch_physops group by query_id) as f group by number order by number asc" > results/num_dist_physops/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct phys_operator) as number from sdss_physops group by query_id) as f group by number order by number asc" > results/num_dist_physops/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct phys_operator) as number from sqlshare_physops group by query_id) as f group by number order by number asc" > results/num_dist_physops/sqlshare.csv
```

### Table touch

```sql
select number, count(*) from (select query_id, count(*) as number from tpch_tables group by query_id) as foo group by number order by number asc;
select number, count(*) from (select query_id, count(distinct "table") as number from tpch_tables group by query_id) as foo group by number order by number asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct \"table\") as number from tpch_tables group by query_id) as foo group by number order by number asc" > results/table_touch/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct \"table\") as number from sdss_tables group by query_id) as foo group by number order by number asc" > results/table_touch/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct \"table\") as number from sqlshare_tables group by query_id) as foo group by number order by number asc" > results/table_touch/sqlshare.csv
```

### Column touch

```sql
select number, count(*) from (select query_id, count(*) as number from tpch_columns group by query_id) as foo group by number order by number asc;
select number, count(*) from (select query_id, count(distinct "column") as number from tpch_columns where "table" != 'None' group by query_id) as foo group by number order by number asc;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct \"column\") as number from tpch_columns where \"table\" != 'None' group by query_id) as foo group by number order by number asc" > results/column_touch/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct \"column\") as number from sdss_columns where \"table\" != 'None' group by query_id) as foo group by number order by number asc" > results/column_touch/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select number, count(*) from (select query_id, count(distinct \"column\") as number from sqlshare_columns where \"table\" != 'None' group by query_id) as foo group by number order by number asc" > results/column_touch/sqlshare.csv
```

### List of physical operators ordered by number of occurrences

```sql
select phys_operator, count(*) from tpch_physops group by phys_operator order by count(*) desc limit 10;
```

```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "select phys_operator, count(*) from tpch_physops group by phys_operator order by count(*) desc limit 10" > results/physops/tpch.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select phys_operator, count(*) from sdss_physops group by phys_operator order by count(*) desc limit 10" > results/physops/sdss.csv
sudo -u postgres psql -d sdsslogs -A -F"," -c "select phys_operator, count(*) from sqlshare_physops group by phys_operator order by count(*) desc limit 10" > results/physops/sqlshare.csv
```

Get all ops at once

```sql
select phys_operator, sum(num_tpch) as count_tpch, sum(num_sdss) as count_sdss, sum(num_sqlshare) as count_sqlshare from
  (select id, phys_operator, 1 as num_tpch, 0 as num_sdss, 0 as num_sqlshare from tpch_physops
   union
   select id, phys_operator, 0 as num_tpch, 1 as num_sdss, 0 as num_sqlshare from sdss_physops
   union
   select id, phys_operator, 0 as num_tpch, 0 as num_sdss, 1 as num_sqlshare from sqlshare_physops) as u
group by phys_operator;
```


### Expression operators

```sql
select class, operator, count(*) from expr_ops group by class, operator order by class, operator;
 ```


### Dataset connect graph

```sql
select t1.table || '--' || t2.table || ' [label=' || count(*) || '];' from sqlshare_tables_distinct t1, sqlshare_tables_distinct t2 where t1.query_id = t2.query_id and t1.table != t2.table group by t1.table, t2.table;
```

```bash
sudo -u postgres psql -d sdsslogs -A -c "select distinct \"table\" || ' [label=\"'|| \"table\" || '\"];' from sqlshare_tables;" > results/table_connect_sqlshare.dot

sudo -u postgres psql -d sdsslogs -A -c "select t1.table || '--' || t2.table || ' [label=' || count(*) || '];' from sqlshare_tables_distinct t1, sqlshare_tables_distinct t2 where t1.query_id = t2.query_id and t1.table != t2.table group by t1.table, t2.table;" >> results/table_connect_sqlshare.dot
```

### Query connect graph

```sql
select t1.query_id || '--' || t2.query_id || ' [label=' || count(*) || '];' from sqlshare_tables_distinct t1, sqlshare_tables_distinct t2 where t1.query_id != t2.query_id and t1.table = t2.table group by t1.query_id, t2.query_id;
```

```bash
sudo -u postgres psql -d sdsslogs -A -c "select distinct query_id || ' [label=\"'|| query_id || '\"];' from sqlshare_tables;" > results/qConnectSQLShare.dot

sudo -u postgres psql -d sdsslogs -A -c "select t1.query_id || '--' || t2.query_id || ' [label=' || count(*) || '];' from sqlshare_tables_distinct t1, sqlshare_tables_distinct t2 where t1.query_id != t2.query_id and t1.table = t2.table group by t1.query_id, t2.query_id;" >> results/qConnectSQLShare.dot
```

### Q vs D plot (Number of distinct datasets vs Number of distint Queries)
```bash
sudo -u postgres psql -d sdsslogs -A -F"," -c "with D as (SELECT owner, count(distinct(query)) as D from sqlshare_logs where has_plan = true and isview = true group by owner) select sqlshare_logs.owner, count(distinct(plan)) as Q, D.D from sqlshare_logs, D where D.owner = sqlshare_logs.owner group by sqlshare_logs.owner, D.D" > /home/shrainik/user_Q_D.csv
```
