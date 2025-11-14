package endpointgroupbinding

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/aws/smithy-go"
	endpointgroupbindingv1alpha1 "github.com/h3poteto/aws-global-accelerator-controller/pkg/apis/endpointgroupbinding/v1alpha1"
	cloudaws "github.com/h3poteto/aws-global-accelerator-controller/pkg/cloudprovider/aws"
	"github.com/h3poteto/aws-global-accelerator-controller/pkg/reconcile"
	"golang.org/x/exp/maps"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const finalizer = "operator.h3poteto.dev/endpointgroupbindings"

func (c *EndpointGroupBindingController) reconcile(ctx context.Context, obj *endpointgroupbindingv1alpha1.EndpointGroupBinding) (reconcile.Result, error) {
	cloud, err := cloudaws.NewAWS("us-west-2")
	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	if obj.DeletionTimestamp != nil {
		return c.reconcileDelete(ctx, obj, cloud)
	}
	if len(obj.Finalizers) == 0 {
		return c.reconcileCreate(ctx, obj, cloud)
	}
	return c.reconcileUpdate(ctx, obj, cloud)
}

func (c *EndpointGroupBindingController) reconcileDelete(ctx context.Context, obj *endpointgroupbindingv1alpha1.EndpointGroupBinding, cloud *cloudaws.AWS) (reconcile.Result, error) {
	if len(obj.Status.EndpointIds) == 0 {
		copied := obj.DeepCopy()
		copied.Finalizers = []string{}
		_, err := c.client.OperatorV1alpha1().EndpointGroupBindings(copied.Namespace).Update(ctx, copied, metav1.UpdateOptions{})
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	// Resolve the endpoint group ARN
	endpointGroupArn, err := c.resolveEndpointGroupArn(ctx, obj, cloud)
	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	endpoint, err := cloud.DescribeEndpointGroup(ctx, endpointGroupArn)
	if err != nil {
		// If the endpoint group is not found, we should remove the finalizer and update the object.
		var awsErr smithy.APIError
		if errors.As(err, &awsErr) {
			klog.V(1).Infof("Failed to get EndpointGroup %s: %s", endpointGroupArn, awsErr.ErrorCode())
			if awsErr.ErrorCode() == cloudaws.ErrEndpointGroupNotFoundException {
				copied := obj.DeepCopy()
				copied.Finalizers = []string{}
				_, err := c.client.OperatorV1alpha1().EndpointGroupBindings(copied.Namespace).Update(ctx, copied, metav1.UpdateOptions{})
				if err != nil {
					klog.Error(err)
					return reconcile.Result{}, err
				}
				return reconcile.Result{}, nil
			}
		}

		klog.Error(err)
		return reconcile.Result{}, err

	}
	endpointIds := obj.Status.EndpointIds
	for i := range obj.Status.EndpointIds {
		id := obj.Status.EndpointIds[i]
		region := cloudaws.GetRegionFromARN(id)
		cloud, err := cloudaws.NewAWS(region)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}

		err = cloud.RemoveLBFromEdnpointGroup(ctx, endpoint, id)
		if err != nil {
			return reconcile.Result{}, err
		}
		endpointIds = append(endpointIds[:i], endpointIds[i+1:]...)
	}

	copied := obj.DeepCopy()
	copied.Status.EndpointIds = endpointIds
	copied.Status.ObservedGeneration = obj.Generation
	_, err = c.client.OperatorV1alpha1().EndpointGroupBindings(copied.Namespace).UpdateStatus(ctx, copied, metav1.UpdateOptions{})
	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	return reconcile.Result{Requeue: true, RequeueAfter: 1 * time.Second}, nil
}

func (c *EndpointGroupBindingController) reconcileCreate(ctx context.Context, obj *endpointgroupbindingv1alpha1.EndpointGroupBinding, _ *cloudaws.AWS) (reconcile.Result, error) {
	copied := obj.DeepCopy()
	copied.Finalizers = []string{finalizer}

	_, err := c.client.OperatorV1alpha1().EndpointGroupBindings(copied.Namespace).Update(ctx, copied, metav1.UpdateOptions{})
	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (c *EndpointGroupBindingController) reconcileUpdate(ctx context.Context, obj *endpointgroupbindingv1alpha1.EndpointGroupBinding, cloud *cloudaws.AWS) (reconcile.Result, error) {
	// Check the different between the current service/ingress load balancer ARNs and status.endpointIds
	// If the ARN is not in the status.endpointIds, add it to the endpoint group
	// If status.endpointIds is not in the ARN, remove it from the endpoint group

	// First, resolve the endpoint group ARN
	endpointGroupArn, err := c.resolveEndpointGroupArn(ctx, obj, cloud)
	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	// Update status if the resolved ARN changed
	if obj.Status.ResolvedEndpointGroupArn != endpointGroupArn {
		copied := obj.DeepCopy()
		copied.Status.ResolvedEndpointGroupArn = endpointGroupArn
		_, err = c.client.OperatorV1alpha1().EndpointGroupBindings(copied.Namespace).UpdateStatus(ctx, copied, metav1.UpdateOptions{})
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		// Re-fetch the object after status update
		obj, err = c.client.OperatorV1alpha1().EndpointGroupBindings(obj.Namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
	}

	arns := map[string]string{}
	hostnames, err := c.getLoadBalancerHostName(obj)
	if err != nil {
		return reconcile.Result{}, err
	}
	var regionalCloud *cloudaws.AWS
	for _, hostname := range hostnames {
		name, region, err := cloudaws.GetLBNameFromHostname(hostname)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		regionalCloud, err = cloudaws.NewAWS(region)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		lb, err := regionalCloud.GetLoadBalancer(ctx, name)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		arns[*lb.LoadBalancerArn] = name
	}
	klog.V(4).Infof("Service LoadBalancer ARNs: %v", arns)

	newEndpointIds := []string{}
	removedEndpointIds := []string{}
	for arn, _ := range arns {
		if !slices.Contains(obj.Status.EndpointIds, arn) {
			newEndpointIds = append(newEndpointIds, arn)
		}
	}
	for _, endpointId := range obj.Status.EndpointIds {
		if !slices.Contains(maps.Keys(arns), endpointId) {
			removedEndpointIds = append(removedEndpointIds, endpointId)
		}
	}
	klog.V(4).Infof("New EndpointIds: %v", newEndpointIds)
	klog.V(4).Infof("Removed EndpointIds: %v", removedEndpointIds)
	if len(newEndpointIds) == 0 && len(removedEndpointIds) == 0 && obj.Status.ObservedGeneration == obj.Generation {
		return reconcile.Result{}, nil
	}

	endpointGroup, err := cloud.DescribeEndpointGroup(ctx, endpointGroupArn)
	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	results := obj.Status.EndpointIds

	for _, endpointId := range removedEndpointIds {
		err := regionalCloud.RemoveLBFromEdnpointGroup(ctx, endpointGroup, endpointId)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		results = slices.DeleteFunc(results, func(e string) bool {
			return e == endpointId
		})
	}

	for _, endpointId := range newEndpointIds {
		id, retry, err := regionalCloud.AddLBToEndpointGroup(ctx, endpointGroup, arns[endpointId], obj.Spec.ClientIPPreservation, obj.Spec.Weight)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
		if retry > 0 {
			return reconcile.Result{
				Requeue:      true,
				RequeueAfter: retry,
			}, nil
		}
		if id != nil {
			results = append(results, *id)
		}
	}

	// Check weight of the endpoint
	for id, _ := range arns {
		err := regionalCloud.UpdateEndpointWeight(ctx, endpointGroup, id, obj.Spec.Weight)
		if err != nil {
			klog.Error(err)
			return reconcile.Result{}, err
		}
	}

	copied := obj.DeepCopy()
	copied.Status.EndpointIds = results
	copied.Status.ObservedGeneration = obj.Generation
	_, err = c.client.OperatorV1alpha1().EndpointGroupBindings(copied.Namespace).UpdateStatus(ctx, copied, metav1.UpdateOptions{})

	if err != nil {
		klog.Error(err)
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (c *EndpointGroupBindingController) getLoadBalancerHostName(obj *endpointgroupbindingv1alpha1.EndpointGroupBinding) ([]string, error) {
	hostnames := []string{}
	if obj.Spec.ServiceRef != nil {
		service, err := c.serviceLister.Services(obj.Namespace).Get(obj.Spec.ServiceRef.Name)
		if err != nil {
			klog.Error(err)
			return []string{}, err
		}
		if len(service.Status.LoadBalancer.Ingress) < 1 {
			klog.Warningf("%s/%s does not have ingress LoadBalancer, so skip it", service.Namespace, service.Name)
			return []string{}, nil
		}
		for i := range service.Status.LoadBalancer.Ingress {
			hostnames = append(hostnames, service.Status.LoadBalancer.Ingress[i].Hostname)
		}
	} else if obj.Spec.IngressRef != nil {
		ingress, err := c.ingressLister.Ingresses(obj.Namespace).Get(obj.Spec.IngressRef.Name)
		if err != nil {
			klog.Error(err)
			return []string{}, err
		}
		if len(ingress.Status.LoadBalancer.Ingress) < 1 {
			klog.Warningf("%s/%s does not have ingress LoadBalancer, so skip it", ingress.Namespace, ingress.Name)
			return []string{}, nil
		}
		for i := range ingress.Status.LoadBalancer.Ingress {
			hostnames = append(hostnames, ingress.Status.LoadBalancer.Ingress[i].Hostname)
		}
	} else {
		klog.Errorf("EndpointGroupBinding %s does not have serviceRef or ingressRef", obj.Name)
		return []string{}, nil
	}
	return hostnames, nil
}

// resolveEndpointGroupArn resolves the endpoint group ARN from the spec.
// It supports three modes:
// 1. Direct mode: spec.endpointGroupArn is specified
// 2. Accelerator ARN mode: spec.acceleratorArn is specified, controller gets/creates endpoint group
// 3. Name-based mode: spec.globalAcceleratorName is specified, controller finds GA by name
func (c *EndpointGroupBindingController) resolveEndpointGroupArn(ctx context.Context, obj *endpointgroupbindingv1alpha1.EndpointGroupBinding, cloud *cloudaws.AWS) (string, error) {
	// Mode 1: Direct endpoint group ARN
	if obj.Spec.EndpointGroupArn != "" {
		klog.V(4).Infof("Using direct endpoint group ARN: %s", obj.Spec.EndpointGroupArn)
		return obj.Spec.EndpointGroupArn, nil
	}

	// Get region from load balancer (needed for modes 2 & 3)
	hostnames, err := c.getLoadBalancerHostName(obj)
	if err != nil {
		return "", err
	}
	if len(hostnames) == 0 {
		return "", errors.New("no load balancer hostname found")
	}
	_, region, err := cloudaws.GetLBNameFromHostname(hostnames[0])
	if err != nil {
		klog.Errorf("Failed to get region from hostname %s: %v", hostnames[0], err)
		return "", err
	}

	var acceleratorArn string

	// Mode 2: Accelerator ARN - use directly
	if obj.Spec.AcceleratorArn != "" {
		klog.Infof("Using provided accelerator ARN: %s", obj.Spec.AcceleratorArn)
		acceleratorArn = obj.Spec.AcceleratorArn
	} else if obj.Spec.GlobalAcceleratorName != "" {
		// Mode 3: Name-based - find GA by name
		klog.Infof("Looking up Global Accelerator by name: %s", obj.Spec.GlobalAcceleratorName)
		accelerator, err := cloud.FindGlobalAcceleratorByName(ctx, obj.Spec.GlobalAcceleratorName)
		if err != nil {
			klog.Errorf("Failed to find Global Accelerator by name %s: %v", obj.Spec.GlobalAcceleratorName, err)
			return "", err
		}

		if accelerator == nil {
			return "", errors.New("Global Accelerator with name " + obj.Spec.GlobalAcceleratorName + " not found. " +
				"Create it first via annotations in the primary region, or specify acceleratorArn instead")
		}

		klog.Infof("Found Global Accelerator %s with ARN: %s", obj.Spec.GlobalAcceleratorName, *accelerator.AcceleratorArn)
		acceleratorArn = *accelerator.AcceleratorArn
	} else {
		// No mode specified - error
		return "", errors.New("one of spec.endpointGroupArn, spec.acceleratorArn, or spec.globalAcceleratorName must be specified")
	}

	// For modes 2 & 3: Get listener and ensure endpoint group exists
	listener, err := cloud.GetListener(ctx, acceleratorArn)
	if err != nil {
		klog.Errorf("Failed to get listener for accelerator %s: %v", acceleratorArn, err)
		return "", err
	}

	// Ensure endpoint group exists for this region
	endpointGroupArn, created, err := cloud.EnsureEndpointGroupForRegion(ctx, *listener.ListenerArn, region)
	if err != nil {
		klog.Errorf("Failed to ensure endpoint group for region %s: %v", region, err)
		return "", err
	}

	if created {
		klog.Infof("Created new endpoint group %s for region %s", endpointGroupArn, region)
	} else {
		klog.V(4).Infof("Using existing endpoint group %s for region %s", endpointGroupArn, region)
	}

	return endpointGroupArn, nil
}
