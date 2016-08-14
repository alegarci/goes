package main

import (
	. "github.com/goes/indexrs"
)

//  NewBulkIndexrClient
func NewBulkIndexrClient(index, indexType string, maxConns, maxDocs, timeout int) Indexr {
	return NewBulkIndexr(index, indexType, maxConns, maxDocs, timeout)
}
