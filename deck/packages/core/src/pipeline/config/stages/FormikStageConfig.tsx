import type { FormikErrors, FormikProps } from 'formik';
import { cloneDeep, isEqual, isPlainObject, mergeWith, pickBy } from 'lodash';
import React from 'react';

import type { Application } from '../../../application';
import type { IPipeline, IStage, ITrigger } from '../../../domain';
import { LayoutProvider, ResponsiveFieldLayout, SpinFormik, WatchValue } from '../../../presentation';

export interface IFormikStageConfigInjectedProps {
  application: Application;
  pipeline: IPipeline;
  formik: FormikProps<IStage>;
}

export type IContextualValidator = (
  values: IStage | ITrigger,
  context: any,
) => FormikErrors<IStage> | Promise<FormikErrors<IStage>>;

export interface IFormikStageConfigProps {
  application: Application;
  stage: IStage;
  pipeline: IPipeline;
  validate?: IContextualValidator;
  render: (props: IFormikStageConfigInjectedProps) => React.ReactNode;
  onChange?: (changes: Partial<IStage>) => void;
}

export type IStageValidator = (values: IStage) => void | object | Promise<FormikErrors<IStage>>;

const decorate = (validate: IContextualValidator, props: IFormikStageConfigProps): IStageValidator => {
  const { application, pipeline } = props;
  return (values: IStage) => validate(values, { application, pipeline });
};

// Reports only the fields the form changed, so stage edits made outside the form are kept
const getChangedValues = (values: IStage, prevValues: IStage): Partial<IStage> => {
  const changes: Partial<IStage> = {};
  new Set([...Object.keys(prevValues), ...Object.keys(values)]).forEach((key) => {
    if (values[key] !== prevValues[key]) {
      changes[key] = values[key];
    }
  });
  return changes;
};

export class FormikStageConfig extends React.Component<IFormikStageConfigProps> {
  private readonly pipelineStage: IStage;
  // Defaults the stage config added to its copy of the pipeline stage
  private addedValues: Partial<IStage>;

  constructor(props: IFormikStageConfigProps) {
    super(props);
    const { pipeline, stage } = props;
    const pipelineStage = stage.refId ? pipeline?.stages?.find((s) => s.refId === stage.refId) : undefined;
    this.pipelineStage = pipelineStage;
    // Leaves arrays and values the pipeline stage already has
    this.addedValues =
      pipelineStage && pipelineStage !== stage
        ? pickBy(
            stage,
            (value, key) =>
              value != null &&
              (pipelineStage[key] == null || (isPlainObject(value) && !isEqual(value, pipelineStage[key]))),
          )
        : {};
  }

  // Sends the added defaults with the first change, filled into the pipeline stage's current values
  private handleChange = (values: IStage, prevValues: IStage) => {
    const { addedValues, pipelineStage } = this;
    this.addedValues = {};
    const addedChanges: Partial<IStage> = mergeWith(
      cloneDeep(pickBy(pipelineStage, (value, key) => key in addedValues && value != null)),
      addedValues,
      // Keeps values the pipeline stage has, including whole arrays
      (stageValue, addedValue) =>
        stageValue !== undefined && !(isPlainObject(stageValue) && isPlainObject(addedValue)) ? stageValue : undefined,
    );
    this.props.onChange({ ...addedChanges, ...getChangedValues(values, prevValues) });
  };

  public render() {
    const { render, onChange, stage, validate, application, pipeline } = this.props;
    return (
      <SpinFormik<IStage>
        validate={validate && decorate(validate, this.props)}
        initialValues={stage}
        onSubmit={() => {}}
        render={(formik) => (
          <LayoutProvider value={ResponsiveFieldLayout}>
            <WatchValue onChange={onChange && this.handleChange} value={formik.values} />
            {render({ application, pipeline, formik })}
          </LayoutProvider>
        )}
      />
    );
  }
}
