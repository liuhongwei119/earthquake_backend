def test(a):
    a["b"] = "c"
    return a

import numpy as np
if __name__ == '__main__':
    a = np.array([1,2])
    print(a/2)
