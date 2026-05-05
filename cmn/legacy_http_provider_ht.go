// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"net/url"
	"path/filepath"

	"github.com/NVIDIA/aistore/api/apc"
)

// represents ais:// bucket, object and URL associated with a HTTP resource
type HTTPBckObj struct {
	Bck        Bck
	ObjName    string
	OrigURLBck string // HTTP URL of the bucket (object name excluded)
}

////////////////
// HTTPBckObj //
////////////////

func NewHTTPObj(u *url.URL) *HTTPBckObj {
	hbo := &HTTPBckObj{
		Bck: Bck{
			Provider: apc.HT,
			Ns:       NsGlobal,
		},
	}
	hbo.OrigURLBck, hbo.ObjName = filepath.Split(u.Path)
	hbo.OrigURLBck = u.Scheme + apc.BckProviderSeparator + u.Host + hbo.OrigURLBck
	hbo.Bck.Name = OrigURLBck2Name(hbo.OrigURLBck)
	return hbo
}

func NewHTTPObjPath(rawURL string) (*HTTPBckObj, error) {
	urlObj, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return nil, err
	}
	return NewHTTPObj(urlObj), nil
}
