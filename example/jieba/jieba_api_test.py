import jieba

seg_list = jieba.cut("pdd真牛皮", cut_all=True)
print("Full Mode: " + "/ ".join(seg_list))  # 全模式