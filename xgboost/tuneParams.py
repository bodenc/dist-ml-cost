import numpy as np
from sklearn.datasets import load_svmlight_file
from sklearn.metrics import roc_auc_score
import xgboost as xgb
import sys
import time
from hyperopt import hp, fmin, tpe, STATUS_OK, Trials
import datetime



def get_data(myPath):
    data = load_svmlight_file(myPath)
    return data[0], data[1]


train_path = sys.argv[1]
validation_path = sys.argv[2]
output_path = sys.argv[3]

validation_data, validation_labels = get_data(validation_path)

trainDM = xgb.DMatrix(train_path)
validDM = xgb.DMatrix(validation_path)


def objective(params):
    print params    
    num_round = 50

    start = time.time()
    model = xgb.train(params, trainDM, num_round)
    end = time.time()    
    
    pred = model.predict(validDM,ntree_limit=model.best_iteration + 1)
    
    auc = roc_auc_score(validation_labels, pred)
    print "SCORE:", auc, " Train Time:", (end - start)

    return{'loss':1-auc, 'status': STATUS_OK, 'eval_time': (end - start)}  


space = {
    'eta': hp.quniform('eta', 0.01, 0.3, 0.02),
    'max_depth':  hp.choice('max_depth', np.arange(0, 14, dtype=int)),
    'min_child_weight': hp.quniform('min_child_weight', 1, 100, 1),
    'subsample': hp.quniform('subsample', 0.5, 1, 0.1),
    'gamma': hp.quniform('gamma', 0.1, 1, 0.1),
    'colsample_bytree': hp.quniform('colsample_bytree', 0.5, 1, 0.05),
    'alpha' : hp.quniform('alpha', 0.0, 2, 0.1),
    'lambda': hp.quniform('lambda', 0.0, 2, 0.1),
    'eval_metric': 'auc',
    'objective': 'binary:logistic',
    'nthread': 48,
    'booster': 'gbtree',
    'tree_method': 'hist',
    'grow_policy' : 'lossguide',
    'max_leaves': hp.choice('max_leaves', np.arange(0, 255, dtype=int)),
    'silent': 1
}


trials = Trials()
best = fmin(fn=objective,
            space=space,
            algo=tpe.suggest,
            max_evals=5000,
            trials=trials)


print('Finished tuning: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
print best


sorted_results = sorted(trials.trials, key=lambda d: (d['result']['loss'], d['result']['eval_time']))

outputFile = open(output_path, "w")

print "AuC, Time, Vals" + "\n"

for element in sorted_results:
    outputFile.write(str(1-element['result']['loss']) + "," + str(element['result']['eval_time']) + "," + str(element['misc']['vals'])+ "\n") 

for element in sorted_results[:10]:
    print 1-element['result']['loss'], ",", element['result']['eval_time'], ",", element['misc']['vals']

print('Finished tuning: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
print best
