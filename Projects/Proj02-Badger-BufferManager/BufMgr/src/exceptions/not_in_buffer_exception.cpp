/**
 * @auther Hao-Lun Hsu (9081439458)
 *
 */

#include "not_in_buffer_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

NotInBufferException::NotInBufferException(const std::string& nameIn, PageId pageNoIn)
    : BadgerDbException(""), name(nameIn), pageNo(pageNoIn) {
  std::stringstream ss;
  ss << "The page is not present in the buffer pool for file: " << name << "page: " << pageNo;
  message_.assign(ss.str());
}

}