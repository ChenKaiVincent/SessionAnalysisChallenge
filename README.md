# Session Analysis Challenge


## # Files
- input data: data/2015_07_22_mktplace_shop_web_log_sample.log.gz (See https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-file-format)
- main results: outputs/session_analysis_results.txt
- pre-explorations: outputs/exploration.txt (this is the results for answering my own curiosity)

## # How to Run Test cases
- Install the test's dependency: pytest-3.3.2, spark-2.1.1, python 2.7
- sh run_test.sh

## # How to Run Main

- spark-2.1.1-bin-hadoop2.6/bin$ ./spark-submit --master local[2] /SessionAnalysisChallenge/session_analysis/session_analysis.py file:///DataEngineerChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log > /SessionAnalysisChallenge/outputs/session_analysis_results.txt

