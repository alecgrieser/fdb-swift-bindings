/*
* SubspaceTests.swift
*
* This source file is part of the FoundationDB open source project
*
* Copyright 2016-2018 Apple Inc. and the FoundationDB project authors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import Foundation
@testable import FoundationDB
import XCTest

class SubspaceTests: XCTestCase {
	static var allTests: [(String, (SubspaceTests) -> () throws -> Void)] {
		return [
			("testPack", testPack),
			("testContains", testContains),
			("testUnpack", testUnpack),
			("testUnpackOutOfRange", testUnpackOutOfRange),
			("testUncheckedUnpack", testUncheckedUnpack),
			("testImmutableAfterSourceChange", testImmutableAfterSourceChange),
			("testImmutableAfterRawSourceChange", testImmutableAfterRawSourceChange),
			("testNestedSubspaces", testNestedSubspaces),
			("testTupleRange", testTupleRange),
			("testRange", testRange),
			("testEmptyRange", testEmptyRange),
			("testPrefixRange", testPrefixRange),
			("testEmptyPrefixRange", testEmptyPrefixRange),
		]
	}

	func testPack() {
		let tuple = Tuple(1066)
		let subspace = Subspace(tuple)
		XCTAssertEqual(subspace, Subspace(rawPrefix: DatabaseValue(bytes: [0x16, 0x04, 0x2A])))
		XCTAssertEqual(subspace.databaseValue, tuple.databaseValue)

		XCTAssertEqual(subspace.pack(Tuple(1415)), Tuple(1066, 1415).databaseValue)
		XCTAssertEqual(subspace.pack(Tuple("hastings")), Tuple(1066, "hastings").databaseValue)
	}

	func testContains() {
		let subspace = Subspace(Tuple(1066))
		XCTAssertTrue(subspace.contains(Tuple(1066).databaseValue))
		XCTAssertTrue(subspace.contains(Tuple(1066, 1415).databaseValue))
		XCTAssertTrue(subspace.contains(Tuple(1066, "hastings").databaseValue))
		XCTAssertTrue(subspace.contains(DatabaseValue(bytes: [0x16, 0x04, 0x2A, 0xFF])))

		XCTAssertFalse(subspace.contains(DatabaseValue(bytes: [0x16, 0x04])))
		XCTAssertFalse(subspace.contains(Tuple(1065).databaseValue))
		XCTAssertFalse(subspace.contains(Tuple(1067).databaseValue))
	}

	func testUnpack() {
		let subspace = Subspace(Tuple(1066))
		XCTAssertEqual(try subspace.unpack(Tuple(1066, 1415).databaseValue), Tuple(1415))
	}

	func testUnpackOutOfRange() {
		let subspace = Subspace(Tuple(1066))
		XCTAssertThrowsError(try subspace.unpack(Tuple(1415, "agincourt").databaseValue))
	}

	func testUncheckedUnpack() {
		// This only works because (1066,) and (1415,) happen to be the same
		// number of bytes when Tuple packed (i.e., 3 bytes)
		let subspace = Subspace(Tuple(1066))
		XCTAssertEqual(subspace.uncheckedUnpack(Tuple(1415, "hastings").databaseValue), Tuple("hastings"))
	}

	func testImmutableAfterSourceChange() {
		var tuple = Tuple(1066)
		let subspace = Subspace(tuple)
		tuple.append(1415)
		XCTAssertNotEqual(subspace.databaseValue, tuple.databaseValue)
		XCTAssertEqual(subspace.pack(Tuple(1415)), tuple.databaseValue)
	}

	func testImmutableAfterRawSourceChange() {
		var rawPrefix = DatabaseValue(string: "seedString")
		let subspace = Subspace(rawPrefix: rawPrefix)
		rawPrefix.data.append(contentsOf: [0x10, 0x66])
		XCTAssertNotEqual(subspace.databaseValue, rawPrefix)
		XCTAssertEqual(subspace.databaseValue, DatabaseValue(string: "seedString"))
	}

	func testNestedSubspaces() {
		let outerSub = Subspace(Tuple("outer"))
		let innerSub = outerSub.subspace(Tuple("inner"))
		XCTAssertEqual(outerSub.databaseValue, Tuple("outer").databaseValue)
		XCTAssertEqual(innerSub.databaseValue, Tuple("outer", "inner").databaseValue)
		XCTAssertEqual(try outerSub.unpack(innerSub.databaseValue), Tuple("inner"))
	}

	func testTupleRange() {
		let subspace = Subspace(Tuple(1066))
		let subrange = subspace.range(Tuple(1415))

		XCTAssertTrue(subrange.contains(Tuple(1066, 1415, 1588).databaseValue))
		XCTAssertTrue(subrange.contains(Tuple(1066, 1415, false).databaseValue))

		XCTAssertFalse(subrange.contains(Tuple(1066, 1415).databaseValue))
		XCTAssertFalse(subrange.contains(Tuple(1066, 1416).databaseValue))
		XCTAssertFalse(subrange.contains(Tuple(1066, 1415).databaseValue.withSuffix(byte: 0xFF)))
	}

	func testRange() {
		let subspace = Subspace(Tuple(1066))
		let range = subspace.range

		XCTAssertTrue(range.contains(Tuple(1066).appendingNullByte().databaseValue))
		XCTAssertTrue(range.contains(Tuple(1066, 1415).databaseValue))
		XCTAssertTrue(range.contains(DatabaseValue(bytes: [0x16, 0x04, 0x2A, 0xFE])))
		XCTAssertTrue(range.contains(Subspace(rawPrefix: DatabaseValue(bytes: [0x16, 0x04, 0x2A,0xFE])).pack(Tuple(100))))

		XCTAssertFalse(range.contains(Tuple(1065).databaseValue))
		XCTAssertFalse(range.contains(Tuple(1066).databaseValue))
		XCTAssertFalse(range.contains(DatabaseValue(bytes: [0x16, 0x04, 0x2A, 0xFF])))
		XCTAssertFalse(range.contains(Tuple(1067).databaseValue))
		XCTAssertFalse(range.contains(DatabaseValue(bytes: [0x16, 0x04])))
	}

	func testEmptyRange() {
		let subspace = Subspace()
		let range = subspace.range

		XCTAssertEqual(range.lowerBound, DatabaseValue(bytes: [0x00]))
		XCTAssertEqual(range.upperBound, DatabaseValue(bytes: [0xFF]))

		XCTAssertTrue(range.contains(Tuple(1066).databaseValue))

		XCTAssertFalse(range.contains(DatabaseValue(bytes: [])))
		XCTAssertFalse(range.contains(DatabaseValue(bytes: [0xFF])))
	}

	func testPrefixRange() {
		let subspace = Subspace(Tuple(1066))
		let range = subspace.prefixRange

		XCTAssertTrue(range.contains(Tuple(1066).appendingNullByte().databaseValue))
		XCTAssertTrue(range.contains(Tuple(1066, 1415).databaseValue))
		XCTAssertTrue(range.contains(DatabaseValue(bytes: [0x16, 0x04, 0x2A, 0xFE])))
		XCTAssertTrue(range.contains(Subspace(rawPrefix: DatabaseValue(bytes: [0x16, 0x04, 0x2A,0xFE])).pack(Tuple(100))))
		XCTAssertTrue(range.contains(Tuple(1066).databaseValue))
		XCTAssertTrue(range.contains(DatabaseValue(bytes: [0x16, 0x04, 0x2A, 0xFF])))

		XCTAssertFalse(range.contains(Tuple(1065).databaseValue))
		XCTAssertFalse(range.contains(Tuple(1067).databaseValue))
		XCTAssertFalse(range.contains(DatabaseValue(bytes: [0x16, 0x04])))
	}

	func testEmptyPrefixRange() {
		let subspace = Subspace()
		let range = subspace.prefixRange

		XCTAssertTrue(range.contains(DatabaseValue()))
		XCTAssertTrue(range.contains(DatabaseValue(bytes: [0x00])))
		XCTAssertTrue(range.contains(DatabaseValue(bytes: [0xFE])))

		XCTAssertFalse(range.contains(DatabaseValue(bytes: [0xFF])))
	}
}
