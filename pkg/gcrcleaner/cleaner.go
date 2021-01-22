// Copyright 2019 The GCR Cleaner Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gcrcleaner cleans up stale images from a container registry.
package gcrcleaner

import (
	"fmt"
	"path"
	"sort"
	"time"

	gcrauthn "github.com/google/go-containerregistry/pkg/authn"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcrgoogle "github.com/google/go-containerregistry/pkg/v1/google"
	gcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
)

type manifest struct {
	Digest string
	Info   gcrgoogle.ManifestInfo
	Repo   *gcrname.Repository
}

// Cleaner is a gcr cleaner.
type Cleaner struct {
	auther      gcrauthn.Authenticator
	concurrency int
}

// NewCleaner creates a new GCR cleaner with the given token provider and
// concurrency.
func NewCleaner(auther gcrauthn.Authenticator, c int) (*Cleaner, error) {
	return &Cleaner{
		auther:      auther,
		concurrency: c,
	}, nil
}

func (c *Cleaner) getManifestsInSubtree(repo string, recursive bool, manifests []manifest) ([]manifest, error) {
	gcrrepo, err := gcrname.NewRepository(repo)
	if err != nil {
		return []manifest{}, fmt.Errorf("failed to get repo %s: %w", repo, err)
	}

	tags, err := gcrgoogle.List(gcrrepo, gcrgoogle.WithAuth(c.auther))
	if err != nil {
		return []manifest{}, fmt.Errorf("failed to list tags for repo %s: %w", repo, err)
	}

	for k, m := range tags.Manifests {
		manifests = append(manifests, manifest{Digest: k, Info: m, Repo: &gcrrepo})
	}

	if recursive {
		for _, child := range tags.Children {
			manifests, err = c.getManifestsInSubtree(path.Join(repo, child), true, manifests)
			if err != nil {
				return []manifest{}, err
			}
		}
	}

	return manifests, nil
}

// Clean deletes old images from GCR that are (un)tagged and older than "since" and
// higher than the "keep" amount.
func (c *Cleaner) Clean(repo string, since time.Time, allowTagged bool, keep int, recursive bool, dryRun bool) ([]string, error) {
	if recursive {
		fmt.Println("Processing images from whole tree")
	}
	var manifests = []manifest{}
	manifests, err := c.getManifestsInSubtree(repo, recursive, manifests)
	if err != nil {
		return []string{}, err
	}

	var keepCount = 0
	var deleted = []string{}

	// Sort manifest by Created from the most recent to the least
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[j].Info.Created.Before(manifests[i].Info.Created)
	})

	for i, m := range manifests {
		fmt.Printf("Processing %v/%v: %v\n", i, len(manifests), m)
		if c.shouldDelete(m.Info, since, allowTagged) {
			fmt.Printf("Should delete %v\n", m.Info)
			// Keep a certain amount of images
			if keepCount < keep {
				keepCount++
				continue
			}

			if !dryRun {
				// Deletes all tags before deleting the image
				for _, tag := range m.Info.Tags {
					tagged := m.Repo.Tag(tag)
					if err := c.deleteOne(tagged); err != nil {
						return nil, fmt.Errorf("failed to delete %s: %w", tagged, err)
					}
				}
				ref := m.Repo.Digest(m.Digest)
				if err := c.deleteOne(ref); err != nil {
					return []string{}, err
				}
			}

			deleted = append(deleted, m.Digest)
		}
	}

	return deleted, nil
}

// deleteOne deletes a single repo ref using the supplied auth.
func (c *Cleaner) deleteOne(ref gcrname.Reference) error {
	if err := gcrremote.Delete(ref, gcrremote.WithAuth(c.auther)); err != nil {
		return fmt.Errorf("failed to delete %s: %w", ref, err)
	} else {
		fmt.Printf("Successfully deleted %v\n", ref)
	}

	return nil
}

// shouldDelete returns true if the manifest has no tags or allows deletion of tagged images
// and is before the requested time.
func (c *Cleaner) shouldDelete(m gcrgoogle.ManifestInfo, since time.Time, allowTag bool) bool {
	return (allowTag || len(m.Tags) == 0) && m.Uploaded.UTC().Before(since)
}
