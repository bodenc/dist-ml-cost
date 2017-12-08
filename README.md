# Distributed Machine Learning - but at what COST?

This repository contains the code used in the experiments of the [paper](http://learningsys.org/nips17/assets/papers/paper_21.pdf) presented at the [Workshop on ML Systems
at NIPS 2017](http://learningsys.org/nips17/). 

**Contact:** christoph.boden(ät)tu-berlin.de

#### Motivation

Training machine learning models at scale is a popular workload for distributed
data flow systems. However, as these systems were originally built to fulfill quite
different requirements it remains an open question how effectively they actually
perform for ML workloads. In this paper we argue that benchmarking of large scale
ML systems should consider state of the art, single machine libraries as baselines
and sketch such a benchmark for distributed data flow systems.
We present an experimental evaluation of a representative problem for XGBoost,
LightGBM and Vowpal Wabbit and compare them to Apache Spark MLlib with
respect to both: runtime and prediction quality. Our results indicate that while being
able to robustly scale with increasing data set size, current generation data flow
systems are surprisingly inefficient at training machine learning models at need
substantial resources to come within reach of the performance of single machine
libraries.

#### Data Set 

In order to evaluate a relevant and representative standard problem, we choose to use the
[Criteo Click Logs  data set](http://labs.criteo.com/2014/02/kaggle-display-advertising-challenge-dataset/). This clickthrough rate (CTR) prediction data set contains feature values
and click feedback for millions of display ads drawn from a portion of Criteo's traffic over a period
of 24 days. It consists of 13 numeric and 26 categorical features. In its entirety, the data set spawns
about 4 billion data points, has a size of 1.5 TB. For our experiments we sub-sampled the data such
that both classes have equal probability, resulting in roughly 270 million data points and 200 GB of
data in tsv/text format. Given the insights from figure 1, this seems to be a reasonable size. We use
the first 23 days (except day 4 due to a corrupted archive) as training data and day 24 as test data,
ensuring a proper time split.

The preprocessing of the data set was done with Apache Spark with the follwing jobs found [here](https://github.com/bodenc/dist-ml-cost/tree/master/Spark/ml-benchmark/ml-benchmark-spark-jobs/src/main/scala/de/tuberlin/dima/mlbench/spark) :

```
transformLabelsToVW.scala
transformRawToCategoricalLibSVM.scala
transformRawToLibSvm.scala
```

#### Parameter Tuning

Tuning hyperparameters is a crucial part of applying machine learning methods and can have a significant
impact on an algorithms performance. In order to strive for a fair comparison in our experiments
we allotted a fixed and identical timeframe for tuning the parameters to all systems and libraries,
honouring the fact practitioners also face tight deadlines when performing hyperparameter tuning.

As a result the following (hyper)parameters were used in the experiments

**XGBoost** 

```
eta = 0.1
num_round = 500
nthread = 24
min_child_weight = 100
tree_method = hist
grow_policy = lossguide
max_depth = 0
max_leaves = 255
```

**LightGBM**
```
learning_rate = 0.1
num_leaves = 255
num_iterations = 500
num_thread = 24
tree_learner = serial
objective=binary 
```


**Vowpal Wabbit**
```
--loss_function=logistic -b 18 -l 0.3 --initial_t 1 --decay_learning_rate 0.5 --power_t 0.5  --l1 1e-15 --l2 0
```

**Apache Spark LR**
```
RegParam=0.01
```

**Apache Spark GBT**
```
MinInstancesPerNode = 3
MaxDepth = 10
MaxBins = 64
```
 
