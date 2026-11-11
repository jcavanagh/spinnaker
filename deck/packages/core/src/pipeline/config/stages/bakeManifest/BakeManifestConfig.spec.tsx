import { act, fireEvent, render, within } from '@testing-library/react';
import { cloneDeep } from 'lodash';
import React from 'react';

import { BakeManifestConfig } from './BakeManifestConfig';
import { AccountService } from '../../../../account';
import type { IStage } from '../../../../domain';
import { getFormGroupByLabel } from '../../../../utils/testUtils/rtl';

describe('<BakeManifestConfig />', () => {
  const flush = async () => {
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));
    await Promise.resolve();
  };

  const change = async (label: string, role: string, fire: (input: HTMLElement) => void) => {
    const input = within(getFormGroupByLabel(label)).getByRole(role);
    await act(async () => {
      fire(input);
      await flush();
    });
  };

  beforeEach(() => {
    vi.spyOn(AccountService, 'getArtifactAccounts').mockReturnValue(Promise.resolve([]));
  });

  it('changes only the edited fields of a stage', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Bake (Manifest)',
      type: 'bakeManifest',
      requisiteStageRefIds: [],
      expectedArtifacts: [
        {
          id: 'baked-manifest',
          displayName: 'my-app',
          matchArtifact: { type: 'embedded/base64', artifactAccount: 'embedded-artifact', name: 'my-app' },
        },
      ],
      inputArtifacts: [
        {
          account: 'my-git-account',
          artifact: {
            type: 'git/repo',
            artifactAccount: 'my-git-account',
            reference: 'https://example.com/charts.git',
          },
        },
      ],
      helmChartFilePath: 'common',
      namespace: 'my-namespace',
      outputName: 'my-app',
      overrides: { 'containers.app.image': 'my-image' },
      templateRenderer: 'HELM3',
    };
    const original = cloneDeep(stage);
    // Applies the changes to the shared stage, as the pipeline editor does
    const updateStage = (changes: { [key: string]: any }) => Object.assign(stage, changes);
    const { unmount } = render(
      <BakeManifestConfig
        application={{ name: 'app', getDataSource: () => ({ data: [] }) } as any}
        pipeline={{ stages: [stage] } as any}
        stage={stage}
        stageFieldUpdated={() => {}}
        updateStage={updateStage}
        updateStageField={updateStage}
      />,
    );
    await act(flush);

    await change('Namespace', 'textbox', (input) => fireEvent.change(input, { target: { value: 'other-namespace' } }));
    await change('Include CRDs', 'checkbox', (input) => fireEvent.click(input));

    expect(stage).toEqual({ ...original, namespace: 'other-namespace', includeCRDs: true });

    unmount();
  });
});
