if(EXISTS "/home/zks/cmu15445/build/test/recovery_test[1]_tests.cmake")
  include("/home/zks/cmu15445/build/test/recovery_test[1]_tests.cmake")
else()
  add_test(recovery_test_NOT_BUILT recovery_test_NOT_BUILT)
endif()
