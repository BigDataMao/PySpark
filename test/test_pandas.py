import re

str01 = '日均权益分段:0 - 200000:60200000 - 500000:70500000 - 800000:80800000 - 2000000:902000000 - 9999999999:92'
reg = r'(?<=- )\d+:\d{2}'
outList = re.findall(reg,str01)
outLine = '0'
for i in outList:
    j = i.split(':')
    outLine =outLine+'_0.'+j[1]+'/'+j[0]
print(outLine[:-11])