if(EXISTS "/home/zks/cmu15445/build/test/lru_replacer_test[1]_tests.cmake")
  include("/home/zks/cmu15445/build/test/lru_replacer_test[1]_tests.cmake")
else()
  add_test(lru_replacer_test_NOT_BUILT lru_replacer_test_NOT_BUILT)
endif()