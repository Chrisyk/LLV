#include "sca.h"
namespace ConcVLL {

double SCA::analyze(const std::vector<std::string>& reads, const std::vector<std::string>& writes) {

    return static_cast<double>(writes.size()) + 0.1 * reads.size();
}

}
