/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resources

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	cf "github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Cloud Formation Stacks
type CloudFormationStacks struct{}

func (CloudFormationStacks) fetchTags(svc *cf.CloudFormation, stackID string, logger logrus.FieldLogger) ([]Tag, error) {
	var tags []Tag

	err := svc.DescribeStacksPages(&cf.DescribeStacksInput{StackName: aws.String(stackID)},
		func(page *cf.DescribeStacksOutput, _ bool) bool {
			for _, stack := range page.Stacks {
				if aws.StringValue(stack.StackId) != stackID {
					logger.Errorf("unexpected stack id in DescribeStacks output: %s", aws.StringValue(stack.StackId))
					continue
				}
				for _, t := range stack.Tags {
					tags = append(tags, NewTag(t.Key, t.Value))
				}
			}
			return true
		})

	return tags, err
}

func (cfs CloudFormationStacks) MarkAndSweep(opts Options, set *Set) error {
	logger := logrus.WithField("options", opts)
	svc := cf.New(opts.Session, aws.NewConfig().WithRegion(opts.Region))

	var toDelete []*cloudFormationStack // Paged call, defer deletion until we have the whole list.

	pageFunc := func(page *cf.ListStacksOutput, _ bool) bool {
		for _, stack := range page.StackSummaries {
			// Do not delete stacks that are already deleted or are being
			// deleted.
			switch aws.StringValue(stack.StackStatus) {
			case cf.ResourceStatusDeleteComplete,
				cf.ResourceStatusDeleteInProgress:
				continue
			}
			o := &cloudFormationStack{
				account: opts.Account,
				region:  opts.Region,
				id:      aws.StringValue(stack.StackId),
				name:    aws.StringValue(stack.StackName),
			}
			tags, tagErr := cfs.fetchTags(svc, o.id, logger)
			if tagErr != nil {
				logger.Warningf("%s: failed to fetch tags: %v", o.ARN(), tagErr)
				continue
			}
			if !set.Mark(opts, o, stack.CreationTime, tags) {
				continue
			}

			logger.Warningf("%s: deleting %T: %s", o.ARN(), o, o.name)
			if !opts.DryRun {
				toDelete = append(toDelete, o)
			}
		}
		return true
	}

	if err := svc.ListStacksPages(&cf.ListStacksInput{}, pageFunc); err != nil {
		return err
	}

	for _, o := range toDelete {
		if err := o.delete(svc); err != nil {
			logger.Warningf("%s: delete failed: %v", o.ARN(), err)
		}
	}
	return nil
}

func (CloudFormationStacks) ListAll(opts Options) (*Set, error) {
	svc := cf.New(opts.Session, aws.NewConfig().WithRegion(opts.Region))
	set := NewSet(0)
	inp := &cf.ListStacksInput{}

	err := svc.ListStacksPages(inp, func(stacks *cf.ListStacksOutput, _ bool) bool {
		now := time.Now()
		for _, stack := range stacks.StackSummaries {
			arn := cloudFormationStack{
				account: opts.Account,
				region:  opts.Region,
				id:      aws.StringValue(stack.StackId),
				name:    aws.StringValue(stack.StackName),
			}.ARN()
			set.firstSeen[arn] = now
		}

		return true
	})

	return set, errors.Wrapf(err, "couldn't describe cloud formation stacks for %q in %q", opts.Account, opts.Region)
}

type cloudFormationStack struct {
	account string
	region  string
	id      string
	name    string
}

func (p cloudFormationStack) ARN() string {
	return fmt.Sprintf("arn:aws:cloudformation:%s:%s:stack/%s", p.region, p.account, p.id)
}

func (p cloudFormationStack) ResourceKey() string {
	return p.name
}

func (p cloudFormationStack) delete(svc *cf.CloudFormation) error {
	input := &cf.DeleteStackInput{StackName: aws.String(p.name)}
	if _, err := svc.DeleteStack(input); err != nil {
		return err
	}
	return nil
}
