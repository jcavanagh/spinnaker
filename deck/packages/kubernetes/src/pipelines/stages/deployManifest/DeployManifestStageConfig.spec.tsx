import { act, fireEvent, render, within } from '@testing-library/react';
import { cloneDeep } from 'lodash';
import React from 'react';

import type { IStage } from '@spinnaker/core';
import { AccountService, SETTINGS } from '@spinnaker/core';

import { DeployManifestStageConfig } from './DeployManifestStageConfig';
import { getFormGroupByLabel } from '../../../../../core/src/utils/testUtils/rtl';
import { ManifestKindSearchService } from '../../../manifest/ManifestKindSearch';

describe('<DeployManifestStageConfig />', () => {
  let advancedConfiguration: boolean;

  const flush = async () => {
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.resolve();
  };

  const toggle = async (label: string) => {
    const input = within(getFormGroupByLabel(label)).getByRole('checkbox');
    await act(async () => {
      fireEvent.click(input);
      await flush();
    });
  };

  beforeEach(() => {
    advancedConfiguration = SETTINGS.feature.deployManifestStageAdvancedConfiguration;
    SETTINGS.feature.deployManifestStageAdvancedConfiguration = true;
    vi.spyOn(AccountService, 'getAllAccountDetailsForProvider').mockReturnValue(Promise.resolve([]));
    vi.spyOn(AccountService, 'getArtifactAccounts').mockReturnValue(Promise.resolve([]));
    vi.spyOn(ManifestKindSearchService, 'search').mockReturnValue(Promise.resolve([]));
  });

  afterEach(() => {
    SETTINGS.feature.deployManifestStageAdvancedConfiguration = advancedConfiguration;
  });

  it('keeps earlier edits on the stage when later fields are edited', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Deploy (Manifest)',
      type: 'deployManifest',
      requisiteStageRefIds: [],
      manifests: [],
      // Shows the Expression Evaluation field
      source: 'artifact',
    };
    // Applies the changes to the shared stage, as the pipeline editor does
    const updateStage = (changes: { [key: string]: any }) => Object.assign(stage, changes);
    const { unmount } = render(
      <DeployManifestStageConfig
        application={{ getDataSource: () => ({ data: [] }) } as any}
        pipeline={{ stages: [stage] } as any}
        stage={stage}
        stageFieldUpdated={() => {}}
        updateStage={updateStage}
        updateStageField={updateStage}
      />,
    );
    await act(flush);

    await toggle('Expression Evaluation');
    await toggle('Skip Spec Template Labels');

    expect(stage.skipExpressionEvaluation).toBe(true);
    expect(stage.skipSpecTemplateLabels).toBe(true);

    unmount();
  });

  it('saves the defaults of a new stage with the first edit', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Deploy (Manifest)',
      type: 'deployManifest',
      requisiteStageRefIds: [],
      manifests: [],
    };
    const updateStage = (changes: { [key: string]: any }) => Object.assign(stage, changes);
    const { unmount } = render(
      <DeployManifestStageConfig
        application={{ getDataSource: () => ({ data: [] }) } as any}
        pipeline={{ stages: [stage] } as any}
        stage={stage}
        stageFieldUpdated={() => {}}
        updateStage={updateStage}
        updateStageField={updateStage}
      />,
    );
    await act(flush);

    await toggle('Skip Spec Template Labels');

    expect(stage.source).toBe('text');
    expect(stage.cloudProvider).toBe('kubernetes');
    expect(stage.skipSpecTemplateLabels).toBe(true);

    unmount();
  });

  it('changes only the edited fields of a stage that has its defaults', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Deploy (Manifest)',
      type: 'deployManifest',
      requisiteStageRefIds: [],
      account: 'my-account',
      cloudProvider: 'kubernetes',
      manifestArtifactId: 'manifest-artifact',
      moniker: { app: 'app' },
      namespaceOverride: '${namespace}',
      skipExpressionEvaluation: false,
      source: 'artifact',
      trafficManagement: { enabled: false, options: { enableTraffic: false } },
    };
    const original = cloneDeep(stage);
    const updateStage = vi.fn((changes: { [key: string]: any }) => Object.assign(stage, changes));
    const { unmount } = render(
      <DeployManifestStageConfig
        application={{ getDataSource: () => ({ data: [] }) } as any}
        pipeline={{ stages: [stage] } as any}
        stage={stage}
        stageFieldUpdated={() => {}}
        updateStage={updateStage}
        updateStageField={updateStage}
      />,
    );
    await act(flush);

    await toggle('Expression Evaluation');
    await toggle('Skip Spec Template Labels');

    expect(updateStage.mock.calls).toEqual([[{ skipExpressionEvaluation: true }], [{ skipSpecTemplateLabels: true }]]);
    expect(stage).toEqual({ ...original, skipExpressionEvaluation: true, skipSpecTemplateLabels: true });

    unmount();
  });
});
