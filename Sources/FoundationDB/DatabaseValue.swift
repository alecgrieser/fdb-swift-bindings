/*
 * DatabaseValue.swift
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

/**
This type describes a key or value in the database.

It provides a thin wrapper around the raw sequence of bytes.
*/
public struct DatabaseValue: Equatable, Hashable, Comparable, ExpressibleByStringLiteral {
	/** The raw data that is stored in the database. */
	public var data: Data
	
	/**
	This initializer creates an empty tuple.
	*/
	public init() {
		self.data = Data()
	}
	
	/**
	This initializer creates a tuple from raw data from the database.
	
	This is only intended to be used internally when deserializing data.
	*/
	public init(_ rawData: Data) {
		self.data = rawData
	}
	
	public init(bytes: [UInt8]) {
		self.init(Data(bytes: bytes))
	}
	
	
	/**
	This initializer creates a tuple holding a string.
	
	- parameter string:		The string to put in the tuple.
	*/
	public init(string: String) {
		self.init(Data(bytes: Array(string.utf8)))
	}
	
	/**
	This initializer creates a tuple holding a string.
	
	- parameter string:		The string to put in the tuple.
	*/
	public init(stringLiteral string: String) {
		self.init(string: string)
	}
	
	/**
	This initializer creates a tuple holding a string.
	
	- parameter string:		The string to put in the tuple.
	*/
	public init(extendedGraphemeClusterLiteral string: String) {
		self.init(string: string)
	}
	
	/**
	This initializer creates a tuple holding a string.
	
	- parameter string:		The string to put in the tuple.
	*/
	public init(unicodeScalarLiteral string: String) {
		self.init(string: string)
	}
	
	/**
	This method determines if this tuple has another as a prefix.
	
	This is true whenever the raw data for this tuple begins with the same
	bytes as the raw data for the other tuple.
	
	- parameter prefix:		The tuple we are checking as a possible prefix.
	- returns:				Whether this tuple has the other tuple as its
	prefix.
	*/
	public func hasPrefix(_ prefix: DatabaseValue) -> Bool {
		if prefix.data.count > self.data.count { return false }
		for index in 0..<prefix.data.count {
			if self.data[index] != prefix.data[index] { return false }
		}
		return true
	}
	
	/**
	This method gets the hash code for this tuple.
	*/
	public var hashValue: Int {
		return data.hashValue
	}
	
	/**
	This method increments the last byte in this data.
	
	If the last byte is 0xFF, this will wrap it around to 0x00 and increment
	the prior byte.
	*/
	public mutating func increment() {
		let indices = self.data.indices.reversed()
		self.data.withUnsafeMutableBytes {
			(bytes: UnsafeMutablePointer<UInt8>) -> Void in
			for index in indices {
				let pointer = bytes.advanced(by: index)
				let newByte = Int(pointer.pointee) + 1
				if newByte < 256 {
					pointer.pointee = UInt8(newByte)
					return
				}
				else {
					pointer.pointee = 0
				}
			}
		}
	}

	/**
	Add the contents of another database value to the end of this one.

	- parameter value:		The value to append to the end of this value.
	*/
	public mutating func append(_ value: DatabaseValue) {
		data.append(value.data)
	}

	/**
	Add a byte to the end of this value.

	- parameter byte:		The byte to append to the end of this value.
	*/
	public mutating func append(byte: UInt8) {
		data.append(byte)
	}

	/**
	Create a new database value which contains the contents of this
	database value concatenated with the provided database value.

	- parameter suffix:		The suffix to add to the end of the returned value.
	- returns:				A concatentation of this value and the provided suffix.
	*/
	public func withSuffix(_ suffix: DatabaseValue) -> DatabaseValue {
		var newData = Data(capacity: data.count + suffix.data.count)
		newData.append(data)
		newData.append(suffix.data)
		return DatabaseValue(newData)
	}

	/**
	Create a new database value which contains the contents of this
	database value but with the given byte appended to the end.

	- parameter suffix:		The suffix byte to add to the end of the returned value.
	- returns:				A concatentation of this value and the provided suffix.
	*/
	public func withSuffix(byte: UInt8) -> DatabaseValue {
		var newData = Data(capacity: data.count + 1)
		newData.append(data)
		newData.append(byte)
		return DatabaseValue(newData)
	}
}

/**
This method determines if two tuples are equal.

- parameter lhs:		The first tuple.
- parameter rhs:		The second tuple.
*/
public func ==(lhs: DatabaseValue, rhs: DatabaseValue) -> Bool {
	return lhs.data == rhs.data
}

/**
This method gets an ordering for two tuples.

The tuples will be compared based on the bytes in their raw data.

- parameter lhs:		The first tuple in the comparison.
- parameter rhs:		The second tuple in the comparison.
- returns:				The comparison result
*/
public func <(lhs: DatabaseValue, rhs: DatabaseValue) -> Bool {
	return lhs.data.lexicographicallyPrecedes(rhs.data)
}
