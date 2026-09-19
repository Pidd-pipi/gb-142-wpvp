package repository

import "errors"

var ErrNotFound = errors.New("not found")

// ErrClaimStateConflict 表示占位状态已被其它执行流改变，本次流转无效。
var ErrClaimStateConflict = errors.New("dispatch claim state conflict")
