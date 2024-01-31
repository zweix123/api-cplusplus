#include <cassert>
#include <iostream>
#include <limits>
#include <string>

#include "DolphinDB.h"
#include "ScalarImp.h"
#include "Util.h"

using namespace dolphindb;

int main() {
    DBConnection conn;
    try {
        bool ret = conn.connect("127.0.0.1", 8848, "admin", "123456");
        if (!ret) {
            std::cerr << "Failed to connect to the server" << std::endl;
            return ret;
        }
    } catch (std::exception &ex) {
        std::cerr << "Failed to connect with error: " << ex.what() << std::endl;
        return -1;
    }

    std::string script;
    script += "t = streamTable(300000:0, `name`id`value, `LONG`LONG`LONG);\n";
    script += "share t as rt;\n";
    conn.run(script);

    vector<ConstantSP> args;
    args.push_back(new String("rt"));
    args.push_back(new Long(0));
    args.push_back(new Long(1));
    args.push_back(new Long(2));

    const auto tm1 = Util::getEpochTime();

    for (int i = 0; i < 10000; ++i) {
        conn.lowLatencyRequest("tableInsert", args);
    }

    const auto tm2 = Util::getEpochTime();

    std::cout << "Cost: " << (tm2 - tm1) / 1000.0 << " us" << std::endl;

    return 0;
}
