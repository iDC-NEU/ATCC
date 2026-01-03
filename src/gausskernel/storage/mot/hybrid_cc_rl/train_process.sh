#!/bin/bash

set -euo pipefail

cd "/home/zwx/openGauss-server/src/gausskernel/storage/mot/hybrid_cc_rl/"

echo "$(date '+%Y-%m-%d %H:%M:%S') - 激活虚拟环境" >> process.log
source .venv/bin/activate

# 分级分析
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始分级分析" >> process.log
python tools/analyze_tiers.py --update >> process.log 2>&1
if [ $? -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 分级分析失败" >> process.log
    exit 1
fi
echo "$(date '+%Y-%m-%d %H:%M:%S') - 分级分析完成" >> process.log

# 模型训练
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始训练" >> process.log
python train.py >> process.log 2>&1
if [ $? -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 模型训练失败" >> process.log
    exit 1
fi
echo "$(date '+%Y-%m-%d %H:%M:%S') - 模型训练完成" >> process.log

echo "$(date '+%Y-%m-%d %H:%M:%S') - train_process.sh 执行完成" >> process.log
