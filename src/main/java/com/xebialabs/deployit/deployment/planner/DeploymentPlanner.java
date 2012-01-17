package com.xebialabs.deployit.deployment.planner;

import static com.google.common.collect.Lists.newArrayList;
import static com.xebialabs.deployit.plugin.api.deployment.execution.Plans.interleaved;
import static com.xebialabs.deployit.plugin.api.deployment.execution.Plans.newInterleavedPlan;
import static com.xebialabs.deployit.plugin.api.deployment.execution.Plans.parallel;
import static com.xebialabs.deployit.plugin.api.deployment.execution.Plans.serial;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ListMultimap;
import com.xebialabs.deployit.plugin.api.deployment.execution.DeploymentStep;
import com.xebialabs.deployit.plugin.api.deployment.execution.InterleavedPlan;
import com.xebialabs.deployit.plugin.api.deployment.execution.ParallelPlan;
import com.xebialabs.deployit.plugin.api.deployment.execution.Plan;
import com.xebialabs.deployit.plugin.api.deployment.execution.Plans.InterleavedPlanBuilder;
import com.xebialabs.deployit.plugin.api.deployment.execution.SerialPlan;
import com.xebialabs.deployit.plugin.api.deployment.planning.DeploymentPlanningContext;
import com.xebialabs.deployit.plugin.api.deployment.specification.Delta;
import com.xebialabs.deployit.plugin.api.deployment.specification.DeltaSpecification;
import com.xebialabs.deployit.plugin.api.deployment.specification.Deltas;
import com.xebialabs.deployit.plugin.api.deployment.specification.Operation;
import com.xebialabs.deployit.plugin.api.reflect.Descriptor;
import com.xebialabs.deployit.plugin.api.reflect.DescriptorRegistry;
import com.xebialabs.deployit.plugin.api.reflect.Type;
import com.xebialabs.deployit.plugin.api.udm.Deployed;

public class DeploymentPlanner implements Planner {
    private Set<Method> contributors;
    private ListMultimap<Operation, Method> typeContributors;
    private List<Method> preProcessors;
    private List<Method> postProcessors;
    private Orchestrator orchestrator = new TagBasedOrchestrator();

    private DeploymentPlanner() {
    }

    @Override
    public Plan plan(DeltaSpecification spec) {
        Plan plan = orchestrate(spec);
        Plan preprocessed = preProcessPlan(spec);
        Plan postprocessed = postProcessPlan(spec);
        plan = resolvePlan(plan, spec);
        return serial(preprocessed, plan, postprocessed);
    }

    private Plan postProcessPlan(DeltaSpecification spec) {
        return processPlan(spec, postProcessors);
    }

    private Plan preProcessPlan(DeltaSpecification spec) {
        return processPlan(spec, preProcessors);
    }

    private Plan processPlan(DeltaSpecification spec, List<Method> processors) {
        InterleavedPlanBuilder builder = newInterleavedPlan(interleaved());
        for (Method processor : processors) {
            try {
                Object o = processor.getDeclaringClass().newInstance();
                addResultingStepToBuilder(processor.invoke(o, spec), builder, processor);
            } catch (InstantiationException e) {
                throw new PlannerException(e);
            } catch (IllegalAccessException e) {
                throw new PlannerException(e);
            } catch (InvocationTargetException e) {
	            handleInvocationTargetException(e);
            }
        }
        return builder.build();
    }

    private Plan resolvePlan(Plan plan, DeltaSpecification spec) {
        if (plan instanceof ParallelPlan) {
            return resolveParallelPlan((ParallelPlan) plan, spec);
        } else if (plan instanceof SerialPlan) {
            return resolveSerialPlan((SerialPlan) plan, spec);
        } else {
            InterleavedPlanBuilder planBuilder = newInterleavedPlan((InterleavedPlan) plan);
            orderedResolution((InterleavedPlan) plan, planBuilder, spec);
            return planBuilder.build();
        }
    }

    private Plan resolveSerialPlan(SerialPlan plan, DeltaSpecification spec) {
        return serial(resolvePlans(plan.getPlans(), spec));
    }

    private Plan resolveParallelPlan(ParallelPlan plan, DeltaSpecification spec) {
        return parallel(resolvePlans(plan.getPlans(), spec));
    }

    private List<Plan> resolvePlans(List<Plan> plans, DeltaSpecification spec) {
        List<Plan> resolvedPlans = newArrayList();
        for (Plan subplan : plans) {
            resolvedPlans.add(resolvePlan(subplan, spec));
        }
        return resolvedPlans;
    }


    private Plan orchestrate(DeltaSpecification spec) {
        return orchestrator.orchestrate(spec);
    }

    private void orderedResolution(InterleavedPlan plan, InterleavedPlanBuilder planBuilder, DeltaSpecification spec) {
        DeploymentPlanningContext context = new DefaultDeploymentPlanningContext(planBuilder, spec.getDeployedApplication());
        callTypeContributors(typeContributors, plan, planBuilder, context);
        callContributors(contributors, plan, planBuilder, context);
    }

    private void callTypeContributors(ListMultimap<Operation, Method> typeContributors, InterleavedPlan plan, InterleavedPlanBuilder planBuilder, DeploymentPlanningContext context) {
        if (typeContributors == null) return;
        for (Delta dOp : plan.getDeltas()) {
            List<Method> methods = typeContributors.get(dOp.getOperation());
            @SuppressWarnings("rawtypes")
            Deployed deployed = getActiveDeployed(dOp);
            for (Method method : methods) {
                Type type = Type.valueOf(method.getDeclaringClass());
                if (type.equals(deployed.getType())) {
                    invokeTypeContributer(context, dOp, method);
                } else {
                    Descriptor descriptor = DescriptorRegistry.getDescriptor(deployed.getType());
                    if (descriptor.isAssignableTo(type)) {
                        invokeTypeContributer(context, dOp, method);
                    }
                }
            }
        }
    }

    private void callContributors(Set<Method> methods, InterleavedPlan plan, InterleavedPlanBuilder planBuilder, DeploymentPlanningContext context) {
        if (methods == null) return;
        Deltas deltas = new Deltas(plan.getDeltas());
        for (Method method : methods) {
            try {
                Object contributorInstance = method.getDeclaringClass().newInstance();
                method.invoke(contributorInstance, deltas, context);
            } catch (InstantiationException e) {
                throw new PlannerException(e);
            } catch (IllegalAccessException e) {
                throw new PlannerException(e);
            } catch (InvocationTargetException e) {
	            handleInvocationTargetException(e);
            }
        }
    }

    private Deployed<?, ?> getActiveDeployed(Delta dOp) {
        if (dOp.getOperation() == Operation.DESTROY) {
            return dOp.getPrevious();
        }
        return dOp.getDeployed();
    }

    private void invokeTypeContributer(DeploymentPlanningContext planContext, Delta delta, Method method) {
        try {
            if (method.getParameterTypes().length == 2) {
                method.invoke(getActiveDeployed(delta), planContext, delta);
            } else {
                method.invoke(getActiveDeployed(delta), planContext);
            }
        } catch (IllegalAccessException e) {
            throw new PlannerException(e);
        } catch (InvocationTargetException e) {
	        handleInvocationTargetException(e);
        }
    }

	private void handleInvocationTargetException(InvocationTargetException e) {
		Throwable cause = e.getCause();
		if (cause != null) {
		    throw new PlannerException(cause.getMessage(), cause);
		} else {
		    throw new PlannerException(e);
		}
	}

	@SuppressWarnings("unchecked")
    private void addResultingStepToBuilder(Object result, InterleavedPlanBuilder planBuilder, Method method) {
        if (result == null) return;
        if (result instanceof DeploymentStep) {
            planBuilder.withStep((DeploymentStep) result);
        } else if (result instanceof List) {
            planBuilder.withSteps((List<DeploymentStep>) result);
        } else {
            throw new PlannerException("Result of call of %s is not of type Step of List<Step>.", method);
        }
    }

    public static class DeploymentPlannerBuilder {
        private DeploymentPlanner planner;

        public DeploymentPlannerBuilder() {
            this.planner = new DeploymentPlanner();
        }

        public DeploymentPlannerBuilder typeContributors(ListMultimap<Operation,Method> typeContributors) {
            planner.typeContributors = typeContributors;
            return this;
        }

        public DeploymentPlannerBuilder contributors(Set<Method> contributors) {
            planner.contributors = contributors;
            return this;
        }

        public DeploymentPlannerBuilder preProcessors(List<Method> preProcessors) {
            planner.preProcessors = preProcessors;
            return this;
        }

        public DeploymentPlannerBuilder postProcessors(List<Method> postProcessors) {
            planner.postProcessors = postProcessors;
            return this;
        }

        public DeploymentPlanner build() {
            return planner;
        }
    }

    @SuppressWarnings("serial")
    public static class PlannerException extends RuntimeException {
        public PlannerException() {
        }

        public PlannerException(String message) {
            super(message);
        }

        public PlannerException(String message, Object... params) {
            super(String.format(message, params));
        }

        public PlannerException(String message, Throwable cause) {
            super(message, cause);
        }

        public PlannerException(Throwable cause) {
            super(cause);
        }
    }
}
