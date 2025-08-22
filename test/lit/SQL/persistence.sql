--// Test RocksDB persistence across sessions using actual sql executable
--// This test validates that table schemas created in one session persist and are available in subsequent sessions
--// Note: This test focuses on schema persistence, not data persistence due to current INSERT limitations
--// 
--// RUN: rm -rf %t && mkdir -p %t
--// RUN: echo "create table persist_test( id int, value int, primary key(id) );" | %S/../../../build/lingodb-debug/sql %t/
--// RUN: echo "select * from persist_test;" | %S/../../../build/lingodb-debug/sql %t/ | FileCheck %s --check-prefix=CHECK-FIRST-SESSION
--// 
--// RUN: echo "select * from persist_test;" | %S/../../../build/lingodb-debug/sql %t/ | FileCheck %s --check-prefix=CHECK-SECOND-SESSION  
--// RUN: echo "create table second_table( key_id int, data bigint, primary key(key_id) );" | %S/../../../build/lingodb-debug/sql %t/
--// RUN: echo "select * from second_table;" | %S/../../../build/lingodb-debug/sql %t/ | FileCheck %s --check-prefix=CHECK-SECOND-TABLE
--//
--// RUN: echo "select * from persist_test;" | %S/../../../build/lingodb-debug/sql %t/ | FileCheck %s --check-prefix=CHECK-THIRD-SESSION
--// RUN: echo "select * from second_table;" | %S/../../../build/lingodb-debug/sql %t/ | FileCheck %s --check-prefix=CHECK-BOTH-TABLES
--//
--// RUN: rm -rf %t

--// CHECK-FIRST-SESSION: |                            id  |                         value  |
--// CHECK-FIRST-SESSION: ------------------------------------------------------------------

--// CHECK-SECOND-SESSION: |                            id  |                         value  |
--// CHECK-SECOND-SESSION: ------------------------------------------------------------------

--// CHECK-SECOND-TABLE: |                        key_id  |                          data  |
--// CHECK-SECOND-TABLE: ------------------------------------------------------------------

--// CHECK-THIRD-SESSION: |                            id  |                         value  |
--// CHECK-THIRD-SESSION: ------------------------------------------------------------------

--// CHECK-BOTH-TABLES: |                        key_id  |                          data  |
--// CHECK-BOTH-TABLES: ------------------------------------------------------------------ 