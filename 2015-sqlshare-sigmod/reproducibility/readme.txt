================================================================================
Readme for reproducibility submission of paper ID 63
SQLShare: Results from a Multi-Year SQL-as-a-Service Experiment

A) Source code info
Repository: https://github.com/uwescience/query-workload-analysis
Programming Language: Python
Additional Programming Language info:
Compiler Info: Python2.7
Packages/Libraries Needed: pip, virtualenv (rest of the requirements are installed by the script, sqllite, various python libraries, latex to build the final paper).

B) Datasets info
Repository: https://s3-us-west-2.amazonaws.com/shrquerylogs/sdssquerieswithplan.csv, https://s3-us-west-2.amazonaws.com/shrquerylogs/QueriesWithPlan.csv, https://s3-us-west-2.amazonaws.com/shrquerylogs/ViewsWithPlan.csv
Data generators: N/A

C) Hardware Info
C1) Processor: N/A
C2) Caches: N/A
C3) Memory: 16GB
C4) Secondary Storage: About 25GB.
C5) Network: N/A, internet connectivity required for script to download dataset.

D) Experimentation Info

D1) Get the code: git clone https://github.com/uwescience/query-workload-analysis.git
D2) Software (install virtualenv): pip install virtualenv; virtualenv vqwla

source vqwla/bin/execute


Next, one script does everything, installs packages within virtualenv, downloads the datasets, creates sqlite databases, run experiments, creates graphs and updates the paper.pdf with new graphs.
D3) ./run-experiments.sh 
================================================================================