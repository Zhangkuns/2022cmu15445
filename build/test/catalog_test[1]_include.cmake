if(EXISTS "/home/zks/cmu15445/build/test/catalog_test[1]_tests.cmake")
  include("/home/zks/cmu15445/build/test/catalog_test[1]_tests.cmake")
else()
  add_test(catalog_test_NOT_BUILT catalog_test_NOT_BUILT)
endif()
