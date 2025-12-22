import _ from 'lodash';
import { DataTypes, Model } from 'sequelize';
import { AppError } from '../core/app-error';
import { sequelize } from '../core/utils/connections';

interface DeploymentsInterface extends Model {
    id: number;
    appid: number;
    name: string;
    description: string;
    deployment_key: string;
    last_deployment_version_id: number;
    label_id: number;
    created_at: Date;
    updated_at: Date;
}

export const Deployments = sequelize.define<DeploymentsInterface>(
    'Deployments',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        appid: DataTypes.INTEGER({ length: 10 }),
        name: DataTypes.STRING,
        description: DataTypes.STRING,
        deployment_key: DataTypes.STRING,
        last_deployment_version_id: DataTypes.INTEGER({ length: 10 }),
        label_id: DataTypes.INTEGER({ length: 10 }),
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'deployments',
        underscored: true,
        paranoid: true,
    },
);

export function generateDeploymentsLabelId(deploymentId: number) {
    return sequelize.transaction((t) => {
        return Deployments.findByPk(deploymentId, {
            transaction: t,
            lock: t.LOCK.UPDATE,
        }).then((data) => {
            if (_.isEmpty(data)) {
                throw new AppError('does not find deployment');
            }
            data.label_id += 1;
            return data.save({ transaction: t }).then((d) => {
                return d.label_id;
            });
        });
    });
}
