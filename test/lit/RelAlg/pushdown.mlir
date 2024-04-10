// RUN: mlir-db-opt %s -split-input-file -mlir-print-debuginfo -mlir-print-local-scope  --relalg-pushdown| FileCheck %s
module @querymodule  {
  func.func @query() {
    //CHECK: %0 =  relalg.const_relation columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})]
    //CHECK: %1 =  relalg.const_relation columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})]
    //CHECK: %2 = relalg.selection %0
    //CHECK: %3 = relalg.selection %1
    //CHECK: %4 = relalg.crossproduct %2, %3
  	%0 = relalg.const_relation  columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%1 = relalg.const_relation  columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%2 = relalg.crossproduct %0, %1
  	%3 = relalg.selection %2 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
        tuples.return %6 : i1
  	}
	%4 = relalg.selection %3 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel2::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel2::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
		tuples.return %6 : i1
	}
    %res_table = relalg.materialize %4 [] => [] : !subop.local_table<[],[]>
    subop.set_result 0 %res_table : !subop.local_table<[],[]>
    return
  }
}
// -----
module @querymodule  {
  func.func @query() {
    //CHECK: %0 =  relalg.const_relation columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})]
    //CHECK: %1 =  relalg.const_relation columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})]
    //CHECK: %2 = relalg.selection %0
    //CHECK: %3 = relalg.selection %1
    //CHECK: %4 = relalg.crossproduct %2, %3
    //CHECK: %5 = relalg.selection %4
  	%0 = relalg.const_relation  columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%1 = relalg.const_relation  columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%2 = relalg.crossproduct %0, %1
	%3 = relalg.selection %2 (%arg0: !tuples.tuple) {
		%10 = tuples.getcol %arg0 @constrel::@attr1 : i32
		%11 = tuples.getcol %arg0 @constrel2::@attr1 : i32
		%12 = db.compare eq %10 : i32, %11 : i32
		tuples.return %12 : i1
	}
  	%4 = relalg.selection %3 (%arg0: !tuples.tuple) {
		%10 = tuples.getcol %arg0 @constrel::@attr1 : i32
		%11 = tuples.getcol %arg0 @constrel::@attr2 : i32
		%12 = db.compare eq %10 : i32, %11 : i32
		tuples.return %12 : i1
  	}
	%5 = relalg.selection %4 (%arg0: !tuples.tuple) {
		%10 = tuples.getcol %arg0 @constrel2::@attr1 : i32
		%11 = tuples.getcol %arg0 @constrel2::@attr2 : i32
		%12 = db.compare eq %10 : i32, %11 : i32
		tuples.return %12 : i1
	}
    %res_table = relalg.materialize %5 [] => [] : !subop.local_table<[],[]>
    subop.set_result 0 %res_table : !subop.local_table<[],[]>
    return
  }
}
// -----
module @querymodule  {
  func.func @query() {
    //CHECK: %0 =  relalg.const_relation columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})]
    //CHECK: %1 =  relalg.const_relation columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})]
    //CHECK: %2 = relalg.selection %0
    //CHECK: %3 = relalg.selection %1
    //CHECK: %4 = relalg.join %2, %3
  	%0 = relalg.const_relation  columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%1 = relalg.const_relation  columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%2 = relalg.join %0, %1 (%arg0: !tuples.tuple) {
  		tuples.return
  	}
  	%3 = relalg.selection %2 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
        tuples.return %6 : i1
  	}
	%4 = relalg.selection %3 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel2::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel2::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
		tuples.return %6 : i1
	}
    %res_table = relalg.materialize %4 [] => [] : !subop.local_table<[],[]>
    subop.set_result 0 %res_table : !subop.local_table<[],[]>
    return
  }
}
// -----
module @querymodule  {
  func.func @query() {
    //CHECK: %0 =  relalg.const_relation columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})]
    //CHECK: %1 =  relalg.const_relation columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})]
    //CHECK: %2 = relalg.selection %0
    //CHECK: %3 = relalg.outerjoin %2, %1
    //CHECK: %4 = relalg.selection %3
  	%0 = relalg.const_relation  columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%1 = relalg.const_relation  columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%2 = relalg.outerjoin %0, %1 (%arg0: !tuples.tuple) {
  		tuples.return
  	} mapping: {@outerjoin::@attr1({type = i32})=[@constrel2::@attr1]}
  	%3 = relalg.selection %2 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
        tuples.return %6 : i1
  	}
	%4 = relalg.selection %3 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel2::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel2::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
		tuples.return %6 : i1
	}
    %res_table = relalg.materialize %4 [] => [] : !subop.local_table<[],[]>
    subop.set_result 0 %res_table : !subop.local_table<[],[]>
    return
  }
}

// -----
module @querymodule  {
  func.func @query() {
    //CHECK: %0 =  relalg.const_relation columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})]
    //CHECK: %1 =  relalg.const_relation columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})]
    //CHECK: %2 = relalg.fullouterjoin %0, %1
    //CHECK: %3 = relalg.selection %2
    //CHECK: %4 = relalg.selection %3
  	%0 = relalg.const_relation  columns : [@constrel::@attr1({type = i32}),@constrel::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%1 = relalg.const_relation  columns : [@constrel2::@attr1({type = i32}),@constrel2::@attr2({type = i32})] values: [[1, 1], [2, 2]]
  	%2 = relalg.fullouterjoin %0, %1 (%arg0: !tuples.tuple) {
  		tuples.return
  	} mapping: {@outerjoin::@attr1({type = i32})=[@constrel2::@attr1]}
  	%3 = relalg.selection %2 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
        tuples.return %6 : i1
  	}
	%4 = relalg.selection %3 (%arg0: !tuples.tuple) {
		%4 = tuples.getcol %arg0 @constrel2::@attr1 : i32
		%5 = tuples.getcol %arg0 @constrel2::@attr2 : i32
		%6 = db.compare eq %4 : i32, %5 : i32
		tuples.return %6 : i1
	}
    %res_table = relalg.materialize %4 [] => [] : !subop.local_table<[],[]>
    subop.set_result 0 %res_table : !subop.local_table<[],[]>
    return
  }
}