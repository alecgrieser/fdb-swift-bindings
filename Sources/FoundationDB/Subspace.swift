/*
* Subspace.swift
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
This defines a convenient way to use Tuples or raw byte prefixes to define namespaces.
Once initialized, this will prefix all keys with the serialized Tuple and can also be
used to unpack keys, removing the prefix and returning the Tuple suffix.

As a best API practice, clients should generally use at least one subspace for application
data.
*/
public struct Subspace: Equatable, Hashable {
	private let rawPrefix : DatabaseValue

	/**
	Create a subspace with no prefix (or with a zero-length prefix).
	*/
	public init() {
		rawPrefix = DatabaseValue(Data())
	}

	/**
	Create a subspace with a given prefix. This prefix does not necessarily
	need to be a packed tuple, but it will be prepended to all packed keys
	as if it were.

	- parameter rawPrefix: 		The prefix that keys within this subspace all begin with.
	*/
	public init(rawPrefix: DatabaseValue) {
		self.rawPrefix = rawPrefix
	}

	/**
	Create a subspace with a given Tuple prefix. The Tuple will be prepended
	to all keys when packed and removed from all keys when they are unpacked.

	- parameter prefix: 		The tuple prefix that keys within this subspace all begin with.
	*/
	public init(_ prefix: Tuple) {
		self.init(rawPrefix: prefix.databaseValue)
	}

	/**
	Pack a tuple within this subspace. This produces a database value which is prefixed
	by the given subspace and then suffixed by the database value associated with
	the given tuple.

	- parameter tuple: 			The tuple to add append to the end of the returned value.
	- returns:					The provided tuple packed as a key within this subpsace.
	*/
	public func pack(_ tuple: Tuple) -> DatabaseValue {
		return rawPrefix.withSuffix(tuple.databaseValue)
	}

	/**
	Produce a subspace within this subspace prefixed by the provided tuple. This can
	be used to produce hierarchies of subspaces, with different subspaces partitioned
	by their different tuple prefixes. All of these subspaces will also share a prefix
	from their parent subspace.

	- parameter tuple:			The tuple include as part of the returned subspace's prefix.
	- returns:					A new subspace formed by joining this subspace with the packed tuple.
	*/
	public func subspace(_ tuple: Tuple) -> Subspace {
		return Subspace(rawPrefix: pack(tuple))
	}

	/**
	Tests whether the given key is within this subspace. In particular, it will check to
	see if the key is prefixed by this subspace's prefix.

	- parameter key: 			The value to check for subspace membership.
	- returns:					Whether the key is logically contained in this subspace.
	*/
	public func contains(_ key : DatabaseValue) -> Bool {
		return key.hasPrefix(rawPrefix)
	}

	/**
	Decodes the tuple that is encoded as a key within this subspace. It will
	strip out the subspace prefix, so only the data after the prefix ends
	will be unpacked into a tuple. (In this way, this function serves as the
	inverse of the `pack` function.) If the key does not begin with the subspace
	prefix, it will throw a `KeyOutsideSubspaceError`.

	- parameter key:			The raw value to unpack.
	- returns:					The decoded tuple from the suffix of the given key.
	*/
	public func unpack(_ key: DatabaseValue) throws -> Tuple {
		if !contains(key) {
			throw KeyOutsideSubspaceError(key: key, rawPrefix: rawPrefix)
		}
		return uncheckedUnpack(key)
	}

	/**
	Decodes the tuple that is encoded as a key within this subspace. This does
	not check to see if the key is actually prefixed by this subspace's
	prefix. It will instead just decode whatever is contained in the suffix
	of the key and ignore whatever is at the beginning of the key based on the length
	of this subspace's prefix.

	Most users should use `unpack` instead. This method can be more performant, however,
	if one is already sure that the key is actually within the subspace.

	- parameter key:			The raw value to unpack.
	- returns:					The decded tuple from the suffix of the given key.
	*/
	public func uncheckedUnpack(_ key: DatabaseValue) -> Tuple {
		return Tuple(rawData: key.data.suffix(from: rawPrefix.data.count))
	}

	/**
	Get the range of all keys strictly within this subspace. That is to say, this produces
	the range of keys that are begin with this subspace's prefix but excludes
	the prefix key itself and also excludes any keys that begin with this subspace's prefix
	followed by an `0xFF` byte.
	*/
	public var range: Range<DatabaseValue> {
		return rawPrefix.withSuffix(byte: 0x00) ..< rawPrefix.withSuffix(byte: 0xFF)
	}

	/**
	Get the range of keys strictly within the subrange of this subspace that is prefixed by
	the given tuple. Like the range function, it exludes the key exactly equal to the provided
	tuple packed into a key within this subspace as well as all keys whose first byte
	following the given prefix is `0xFF`.

	- parameter tuple: 	The tuple to use to use to prefix a subrange of this subspace.
	- returns: 			The range of all keys strictly within a subrange prefixed by the given tuple.
	*/
	public func range(_ tuple : Tuple) -> Range<DatabaseValue> {
		return subspace(tuple).range
	}

	/**
	Get the range of all keys that are prefixed by this subspace's prefix.
	This differs from the range method in that it includes the key that is
	equal to this subspace's prefix and also any keys that begin with `0xFF`
	following the prefix.

	If the subspace is the empty key, then the end point is the key consisting
	only of the byte `0xFF`. The returned range would then represent the range
	of all possible user-addressable keys in the database (i.e., it excludes the
	system keyspace).
	*/
	public var prefixRange: Range<DatabaseValue> {
		if rawPrefix.data.count == 0 {
			// If given the empty prefix, return a range over all non-system keys
			return rawPrefix ..< DatabaseValue(bytes: [0xFF])
		}
		var end = DatabaseValue(rawPrefix.data)
		end.increment()
		return rawPrefix ..< end
	}

	/**
	Return a hash based on the subspace's prefix.
	*/
	public var hashValue: Int {
		return rawPrefix.hashValue
	}

	/**
	Get the database value associated with the subspace's prefix.
	The returned value is the same value that is added to the beginning
	of any packed tuples within this subspace.
	*/
	public var databaseValue : DatabaseValue {
		return rawPrefix
	}
}

/**
This method determines if two subspaces are equal. Two subspaces
are considered equal if and only if they share the same prefix.

- parameter lhs:		The first subspace.
- parameter rhs:		The second subspace.
*/
public func ==(lhs: Subspace, rhs: Subspace) -> Bool {
	return lhs.databaseValue == rhs.databaseValue
}

/**
Error that can be thrown from "unpack" if the key is outside
of the subspace's range, i.e., if it does not begin with the subspace's
prefix.
*/
struct KeyOutsideSubspaceError : Error {
	let key : DatabaseValue
	let rawPrefix : DatabaseValue

	init(key : DatabaseValue, rawPrefix : DatabaseValue) {
		self.key = key
		self.rawPrefix = rawPrefix
	}
}
