# 项目路径结构说明

## 项目目录结构

```
hybrid_cc_rl/
├── config/
│   └── default.yaml          # 配置文件（使用相对路径）
├── input/                     # 输入数据目录（事务日志）
│   └── *.csv / *.log         # 日志文件
├── output/                    # 输出目录
│   └── ldt.json              # 训练后的 LDT 策略表
├── src/                       # 源代码目录
│   ├── __init__.py
│   ├── data_loader.py        # 数据加载模块
│   ├── feature_engineer.py   # 特征工程模块
│   ├── reward_calculator.py  # 奖励计算模块
│   ├── optimizer.py          # 策略优化模块
│   ├── ldt_manager.py        # LDT 管理模块
│   └── pipeline.py           # 训练流水线
├── tools/                     # 工具脚本
│   ├── analyze_tiers.py      # 层级划分工具
│   ├── compare_policy.py     # 性能对比工具
│   └── ycsb_log_generator.py # YCSB 日志生成器
├── requirements.txt           # Python 依赖
└── train.py                   # 训练入口脚本
```

## 注意事项

1. **始终在项目根目录运行脚本**，不要在子目录中运行
2. 配置文件中的路径使用相对路径，不要使用绝对路径
3. `input/` 和 `output/` 目录会在需要时自动创建
4. 日志文件支持 `.csv` 和 `.log` 两种扩展名
5. 多个日志文件会被自动合并处理
