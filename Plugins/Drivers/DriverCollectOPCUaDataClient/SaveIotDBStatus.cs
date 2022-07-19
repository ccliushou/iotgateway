using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DriverCollectOPCUaDataClient
{
    public enum SaveIotDBStatus : int
    {
        成功 = 0, 失败 = -1, 无数据 = -2,部分数据保存失败= -3,初始状态 = -99,
    }
}
