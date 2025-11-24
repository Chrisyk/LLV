#ifndef SCA_H
#define SCA_H
#include <vector>
#include <string>

namespace ConcVLL {

class SCA {
public:

    static double analyze(const std::vector<std::string>& reads, const std::vector<std::string>& writes);
};

}

#endif
