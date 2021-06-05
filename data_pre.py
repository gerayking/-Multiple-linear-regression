import numpy as np

datalen = 1000


x = np.random.random(datalen).reshape((1, datalen))*100
x1 = np.random.random(datalen).reshape((1, datalen))*100
delta = np.random.uniform(-100,100, size=(1, datalen))
y = 4 * x + 15 * x1 + delta
x = x.reshape((datalen,1))
x1 = x1.reshape((datalen,1))
y = y.reshape((datalen,1))
all = np.column_stack((x,x1,y))
print(all.shape)
print(all)
np.savetxt("foo.csv", all, delimiter=',', fmt="%.2f")
