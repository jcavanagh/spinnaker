import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { FormikProps } from 'formik';
import { cloneDeep } from 'lodash';
import React from 'react';

import { FormikStageConfig } from './FormikStageConfig';
import type { IStage } from '../../../domain';
import { FormikFormField, TextInput } from '../../../presentation';
import { setupUser } from '../../../utils/testUtils/userEvent';

describe('FormikStageConfig', () => {
  const config = (stage: IStage, onChange: (values: IStage) => void) => (
    <FormikStageConfig
      application={{} as any}
      pipeline={{} as any}
      stage={stage}
      onChange={onChange}
      render={({ formik }) => (
        <>
          <FormikFormField name="command" input={(inputProps) => <TextInput {...inputProps} />} />
          <button type="button" onClick={() => formik.setFieldValue('command', undefined)}>
            Remove command
          </button>
        </>
      )}
    />
  );

  const typeCommand = async (user: ReturnType<typeof setupUser>, value: string) => {
    const input = screen.getByRole('textbox');
    await user.clear(input);
    await user.type(input, value);
  };

  let formik: FormikProps<IStage>;

  const copyConfig = (pipelineStage: IStage, stage: IStage, onChange: (changes: Partial<IStage>) => void) => (
    <FormikStageConfig
      application={{} as any}
      pipeline={(pipelineStage ? { stages: [pipelineStage] } : {}) as any}
      stage={stage}
      onChange={onChange}
      render={(props) => {
        formik = props.formik;
        return <FormikFormField name="command" input={(inputProps) => <TextInput {...inputProps} />} />;
      }}
    />
  );

  const changeCommand = (value: string) => fireEvent.change(screen.getByRole('textbox'), { target: { value } });

  afterEach(cleanup);

  it('keeps stage fields changed outside the form', async () => {
    const user = setupUser();
    const stage: IStage = {
      refId: '1',
      name: 'Script',
      type: 'script',
      requisiteStageRefIds: [],
      command: 'a',
      failPipeline: true,
    };
    // Applies the values to the stage, as the pipeline editor does
    const onChange = (values: IStage) => Object.assign(stage, values);
    const { rerender } = render(config(stage, onChange));

    await typeCommand(user, 'b');
    // Like Execution Options, which edits the stage outside the form
    stage.failPipeline = false;
    rerender(config(stage, onChange));
    await typeCommand(user, 'c');

    await waitFor(() => expect(stage.command).toBe('c'));
    expect(stage.failPipeline).toBe(false);
  });

  it('passes only the changed fields to onChange', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Script',
      type: 'script',
      requisiteStageRefIds: [],
      command: 'a',
      failPipeline: true,
    };
    const onChange = vi.fn();
    render(config(stage, onChange));

    changeCommand('b');

    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(1));
    expect(onChange).toHaveBeenCalledWith({ command: 'b' });
  });

  it('keeps earlier edits when the stage prop is a copy taken at mount', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Script',
      type: 'script',
      requisiteStageRefIds: [],
      command: 'a',
      failPipeline: true,
    };
    // Like stage configs that clone the stage once and pass the clone as the stage prop
    const stageCopy = { ...stage };
    const onChange = (changes: Partial<IStage>) => Object.assign(stage, changes);
    render(copyConfig(undefined, stageCopy, onChange));

    changeCommand('b');
    await waitFor(() => expect(stage.command).toBe('b'));
    act(() => {
      formik.setFieldValue('failPipeline', false);
    });

    await waitFor(() => expect(stage.failPipeline).toBe(false));
    expect(stage.command).toBe('b');
  });

  it('clears a stage field the form removes', async () => {
    const user = setupUser();
    const stage: IStage = { refId: '1', name: 'Script', type: 'script', requisiteStageRefIds: [], command: 'a' };
    const onChange = (values: IStage) => Object.assign(stage, values);
    render(config(stage, onChange));

    await user.click(screen.getByRole('button', { name: 'Remove command' }));

    await waitFor(() => expect(stage.command).toBeUndefined());
  });
  it('sends the values a stage copy adds with the first change only', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Script',
      type: 'script',
      requisiteStageRefIds: [],
      command: 'a',
      moniker: { cluster: 'main' },
      tags: ['one'],
    };
    // Like stage configs that add defaults to a copy of the stage
    const stageCopy: IStage = {
      ...cloneDeep(stage),
      source: 'text',
      moniker: { cluster: 'main', app: 'app' },
      tags: ['one', 'two'],
    };
    const onChange = vi.fn((changes: Partial<IStage>) => Object.assign(stage, changes));
    render(copyConfig(stage, stageCopy, onChange));

    changeCommand('b');
    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(1));
    changeCommand('c');
    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(2));

    expect(onChange.mock.calls).toEqual([
      [{ source: 'text', moniker: { cluster: 'main', app: 'app' }, command: 'b' }],
      [{ command: 'c' }],
    ]);
    expect(stage.tags).toEqual(['one']);
  });

  it('merges added nested values onto the current stage values', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Script',
      type: 'script',
      requisiteStageRefIds: [],
      command: 'a',
      moniker: { cluster: 'main' },
    };
    const stageCopy: IStage = { ...cloneDeep(stage), moniker: { cluster: 'main', app: 'app' } };
    const onChange = (changes: Partial<IStage>) => Object.assign(stage, changes);
    render(copyConfig(stage, stageCopy, onChange));

    // Edited outside the form after the copy was taken
    stage.moniker = { cluster: 'edited' };
    changeCommand('b');

    await waitFor(() => expect(stage.command).toBe('b'));
    expect(stage.moniker).toEqual({ cluster: 'edited', app: 'app' });
  });

  it('keeps a nested array the stage emptied after the copy was taken', async () => {
    const stage: IStage = {
      refId: '1',
      name: 'Script',
      type: 'script',
      requisiteStageRefIds: [],
      command: 'a',
      trafficManagement: { options: { services: ['a', 'b'] } },
    };
    const stageCopy: IStage = {
      ...cloneDeep(stage),
      trafficManagement: { options: { services: ['a', 'b'], namespace: 'default' } },
    };
    const onChange = (changes: Partial<IStage>) => Object.assign(stage, changes);
    render(copyConfig(stage, stageCopy, onChange));

    // Emptied outside the form after the copy was taken
    stage.trafficManagement = { options: { services: [] } };
    changeCommand('b');

    await waitFor(() => expect(stage.command).toBe('b'));
    expect(stage.trafficManagement).toEqual({ options: { services: [], namespace: 'default' } });
  });
});
