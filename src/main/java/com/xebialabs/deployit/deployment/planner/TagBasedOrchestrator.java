package com.xebialabs.deployit.deployment.planner;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Sets.newHashSet;
import static com.xebialabs.deployit.plugin.api.deployment.execution.Plans.interleaved;
import static com.xebialabs.deployit.plugin.api.deployment.execution.Plans.serial;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.xebialabs.deployit.plugin.api.deployment.execution.InterleavedPlan;
import com.xebialabs.deployit.plugin.api.deployment.execution.Plan;
import com.xebialabs.deployit.plugin.api.deployment.specification.Delta;
import com.xebialabs.deployit.plugin.api.deployment.specification.DeltaSpecification;
import com.xebialabs.deployit.plugin.api.udm.Container;
import com.xebialabs.deployit.plugin.api.udm.Deployed;

/**
 * With thanks to Martin van Vliet!
 */
class TagBasedOrchestrator implements Orchestrator {
    static final Logger LOGGER = LoggerFactory.getLogger(TagBasedOrchestrator.class);

    private static final String DEPLOYMENT_GROUP_TAG_PREFIX = "deployment-group-";
    
    public Plan orchestrate(DeltaSpecification spec) {
        LOGGER.info("Orchestrating delta spec " + spec);

        Set<Delta> unallocatedDeltas = newHashSet(spec.getDeltas());
        // modifies unallocatedDeltas
        Map<String, Set<Delta>> bucketsWithDeltas = takeOutDeltasInBuckets(unallocatedDeltas);

        List<Plan> interleavedPlans = Lists.newArrayList();
        for (Entry<String, Set<Delta>> bucketWithDeltas : bucketsWithDeltas.entrySet()) {
            LOGGER.info("Creating interleaved plan for deployment group '{}'", bucketWithDeltas.getKey());
            interleavedPlans.add(interleavedPlan(bucketWithDeltas.getValue()));
        }

        LOGGER.info("Creating interleaved plan for unallocated deltas: {}", unallocatedDeltas);
        interleavedPlans.add(interleavedPlan(unallocatedDeltas));

        return serial(interleavedPlans.toArray(new Plan[0]));
    }

    private static Map<String, Set<Delta>> takeOutDeltasInBuckets(Set<Delta> deltas) {
        Map<String, Set<Delta>> bucketsWithDeltas = newHashMap();
        
        for (Iterator<Delta> iterator = deltas.iterator(); iterator.hasNext();) {
            Delta delta = iterator.next();
            Container container = checkNotNull(getDeployedOrPrevious(delta), "deployedOrPrevious")
                                  .getContainer();
            Set<String> deploymentGroupTags = filter(container.getTags(), new Predicate<String>() {
                    @Override
                    public boolean apply(String input) {
                        return input.startsWith(DEPLOYMENT_GROUP_TAG_PREFIX);
                    }
                });
            if (deploymentGroupTags.size() > 1) {
                LOGGER.warn("Ignoring multiple deployment group tags on '{}': {}", 
                        container, deploymentGroupTags);
            } else if (deploymentGroupTags.size() == 1) {
                String tag = getOnlyElement(deploymentGroupTags);
                if (!bucketsWithDeltas.containsKey(tag)) {
                    LOGGER.debug("Starting new bucket '{}'", tag);
                    bucketsWithDeltas.put(tag, Sets.<Delta>newHashSet());
                }
                LOGGER.debug("Allocated '{}' to bucket '{}'", delta, tag);
                bucketsWithDeltas.get(tag).add(delta);
                iterator.remove();
            }
        }
        return bucketsWithDeltas;
    }
    
    private static Deployed<?, ?> getDeployedOrPrevious(Delta delta) {
        Deployed<?, ?> deployed = delta.getDeployed();
        return ((deployed != null) ? deployed : delta.getPrevious());
    }
    
    private static InterleavedPlan interleavedPlan(Set<Delta> deltas) {
        return interleaved(deltas.toArray(new Delta[0]));
    }
}
